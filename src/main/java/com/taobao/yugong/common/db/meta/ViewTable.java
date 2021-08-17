package com.taobao.yugong.common.db.meta;

import java.util.List;

/**
 * 视图表信息
 *
 * @author agapple 2013-9-3 下午3:02:15
 * @since 3.0.0
 */
public class ViewTable extends Table {

  private String viewName;       // 视图表名
  private List<ColumnMeta> viewColumns;    // 视图字段
  private String primaryKeyIndex; // 原始表主键的index信息

  public ViewTable(String type, String schema, String name) {
    super(type, schema, name);
  }

  public ViewTable(String type, String schema, String name, List<ColumnMeta> primaryKeys, List<ColumnMeta> columns) {
    super(type, schema, name, primaryKeys, columns);
  }

  public String getViewName() {
    return viewName;
  }

  public void setViewName(String viewName) {
    this.viewName = viewName;
  }

  public List<ColumnMeta> getViewColumns() {
    return viewColumns;
  }

  public void setViewColumns(List<ColumnMeta> viewColumns) {
    this.viewColumns = viewColumns;
  }

  public String getPrimaryKeyIndex() {
    return primaryKeyIndex;
  }

  public void setPrimaryKeyIndex(String primaryKeyIndex) {
    this.primaryKeyIndex = primaryKeyIndex;
  }

}
