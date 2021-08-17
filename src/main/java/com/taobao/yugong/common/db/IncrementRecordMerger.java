package com.taobao.yugong.common.db;

import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.IncrementOpType;
import com.taobao.yugong.common.model.record.IncrementRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * <pre>
 * pk相同的多条变更数据合并后的结果是：
 * 1, I
 * 2, U
 * 3, D
 * 如果有一条I，多条U，merge成I;
 * 如果有多条U，取最晚的那条;
 * </pre>
 *
 * @author agapple 2012-10-31 下午05:23:40
 */
public class IncrementRecordMerger {

  /**
   * 将一批数据进行根据table+主键信息进行合并，保证一个表的一个pk记录只有一条结果
   */
  public static List<IncrementRecord> merge(List<IncrementRecord> records) {
    Map<RowKey, IncrementRecord> result = new LinkedHashMap<RowKey, IncrementRecord>();
    for (IncrementRecord record : records) {
      merge(record, result);
    }
    return new LinkedList<IncrementRecord>(result.values());
  }

  public static void merge(IncrementRecord record, Map<RowKey, IncrementRecord> result) {
    IncrementOpType opType = record.getOpType();
    switch (opType) {
      case I:
        mergeInsert(record, result);
        break;
      case U:
        mergeUpdate(record, result);
        break;
      case D:
        mergeDelete(record, result);
        break;
      default:
        break;
    }
  }

  private static void mergeInsert(IncrementRecord record, Map<RowKey, IncrementRecord> result) {
    // insert无主键变更的处理
    RowKey rowKey = new RowKey(record.getSchemaName(), record.getTableName(), record.getPrimaryKeys());
    if (!result.containsKey(rowKey)) {
      result.put(rowKey, record);
    } else {
      IncrementRecord oldrecord = result.get(rowKey);
      // 如果上一条变更是delete的，就直接用insert替换
      if (oldrecord.getOpType() == IncrementOpType.D) {
        result.put(rowKey, record);
      } else if (oldrecord.getOpType() == IncrementOpType.U || oldrecord.getOpType() == IncrementOpType.I) {
        // 因为物化视图无序，会出现update-insert/insert-insert happend

        // logger.warn("update-insert/insert-insert happend. before[{}] , after[{}]",
        // oldrecord, record);
        // 如果上一条变更是update的，就用insert替换，并且把上一条存在而这一条不存在的字段值拷贝到这一条中
        IncrementRecord mergerecord = replaceColumnValue(record, oldrecord);
        result.put(rowKey, mergerecord);
      }
    }
  }

  private static void mergeUpdate(IncrementRecord record, Map<RowKey, IncrementRecord> result) {
    RowKey rowKey = new RowKey(record.getSchemaName(), record.getTableName(), record.getPrimaryKeys());
    if (!result.containsKey(rowKey)) {// 没有主键变更
      result.put(rowKey, record);
    } else {
      IncrementRecord oldrecord = result.get(rowKey);
      // 如果上一条变更是insert的，就把这一条的eventType改成insert，并且把上一条存在而这一条不存在的字段值拷贝到这一条中
      if (oldrecord.getOpType() == IncrementOpType.I) {
        record.setOpType(IncrementOpType.I);

        IncrementRecord mergeRecord = replaceColumnValue(record, oldrecord);
        result.put(rowKey, mergeRecord);
      } else if (oldrecord.getOpType() == IncrementOpType.U) {// 可能存在
        // 1->2
        // ,
        // 2update的问题

        // 如果上一条变更是update的，把上一条存在而这一条不存在的数据拷贝到这一条中
        IncrementRecord mergeRecord = replaceColumnValue(record, oldrecord);
        result.put(rowKey, mergeRecord);
      } else if (oldrecord.getOpType() == IncrementOpType.D) {
        // 异常情况，出现 delete + update，那就直接更新为update
        result.put(rowKey, record);
      }
    }
  }

  private static void mergeDelete(IncrementRecord record, Map<RowKey, IncrementRecord> result) {
    // 只保留pks，把columns去掉. 以后针对数据仓库可以开放delete columns记录
    RowKey rowKey = new RowKey(record.getSchemaName(), record.getTableName(), record.getPrimaryKeys());
    result.put(rowKey, record);
  }

  /**
   * 把old中的值存在而new中不存在的值合并到new中,并且把old中的变更前的主键保存到new中的变更前的主键.
   */
  private static IncrementRecord replaceColumnValue(IncrementRecord newrecord, IncrementRecord oldrecord) {
    List<ColumnValue> newColumns = newrecord.getColumns();
    List<ColumnValue> oldColumns = oldrecord.getColumns();
    List<ColumnValue> temp = new ArrayList<ColumnValue>();
    for (ColumnValue oldColumn : oldColumns) {
      boolean contain = false;
      for (ColumnValue newColumn : newColumns) {
        if (oldColumn.getColumn().getName().equalsIgnoreCase(newColumn.getColumn().getName())) {
          contain = true;
        }
      }

      if (!contain) {
        temp.add(oldColumn);
      }
    }
    newColumns.addAll(temp);
    // 把上一次变更的旧主键传递到这次变更的旧主键.
    return newrecord;
  }

  public static class RowKey implements Serializable {

    private static final long serialVersionUID = -7369951798499581038L;
    private String schemaName;                                     // tableId代表统配符时，需要指定schemaName
    private String tableName;                                      // tableId代表统配符时，需要指定tableName
    private List<ColumnValue> keys = new ArrayList<ColumnValue>();

    public RowKey(String schemaName, String tableName, List<ColumnValue> keys) {
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.keys = keys;
    }

    public RowKey(List<ColumnValue> keys) {
      this.keys = keys;
    }

    public List<ColumnValue> getKeys() {
      return keys;
    }

    public void setKeys(List<ColumnValue> keys) {
      this.keys = keys;
    }

    public String getSchemaName() {
      return schemaName;
    }

    public void setSchemaName(String schemaName) {
      this.schemaName = schemaName;
    }

    public String getTableName() {
      return tableName;
    }

    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((keys == null) ? 0 : keys.hashCode());
      result = prime * result + ((schemaName == null) ? 0 : schemaName.hashCode());
      result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
      return result;
    }

    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      RowKey other = (RowKey) obj;
      if (keys == null) {
        if (other.keys != null) return false;
      } else if (!keys.equals(other.keys)) return false;
      if (schemaName == null) {
        if (other.schemaName != null) return false;
      } else if (!schemaName.equals(other.schemaName)) return false;
      if (tableName == null) {
        if (other.tableName != null) return false;
      } else if (!tableName.equals(other.tableName)) return false;
      return true;
    }

  }
}
