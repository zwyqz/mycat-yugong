package com.taobao.yugong.translator.celoan;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.translator.DictDataTranslator;
import com.taobao.yugong.translator.SourceBackTableDataTranslator;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.sql.Date;
import java.sql.Types;

/**
 * 测试类 回调测试
 *
 * @author wyzhang
 * @date 2020/8/18 16:58
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class ProvinceDataTranslator extends SourceBackTableDataTranslator {
  public static final long CURRENT_TIME = 1599719869901L;
  public static final String PASSWORD = "123456";

  private boolean addCode;

  public void setAddCode(boolean addCode) {
    this.addCode = addCode;
  }

  public void getDictValue(DataSource sourceDs, DataSource targetDs, Record record, String key) {
    if(record.getColumnByName(key).getValue() == null  || StringUtils.isEmpty(record.getColumnByName(key).getValue().toString())) {
      if(addCode) {
        ColumnMeta codeMeta = new ColumnMeta(key + "_code", Types.VARCHAR);
        ColumnValue codeColumn = new ColumnValue(codeMeta, "");
        record.addColumn(codeColumn);
      }
      return ;
    }

    if(null == record.getColumnByName(key)) {
        return ;
    }

    String code = record.getColumnByName(key).getValue().toString();
    String name = DictDataTranslator.getSourceName(code);

    String dict_value = DictDataTranslator.getTargetKey(name);

    record.getColumnByName(key).setValue(dict_value);

    if (addCode) {
      ColumnMeta codeMeta = new ColumnMeta(key + "_code", Types.VARCHAR);
      ColumnValue codeColumn = new ColumnValue(codeMeta, dict_value);
      record.addColumn(codeColumn);
    }
    return ;
  }


  @Override
  public boolean translator(DataSource sourceDs, DataSource targetDs, Record record) {
     getDictValue(sourceDs, targetDs, record, "province");
     getDictValue(sourceDs, targetDs, record, "city");
     getDictValue(sourceDs, targetDs, record, "county");

    return super.translator(record);
  }
}
