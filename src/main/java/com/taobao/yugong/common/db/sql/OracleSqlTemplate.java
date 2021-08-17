package com.taobao.yugong.common.db.sql;

/**
 * http://en.wikipedia.org/wiki/Merge_(SQL)
 *
 * @author agapple 2014年2月25日 下午11:38:06
 * @since 1.0.0
 */
public class OracleSqlTemplate extends SqlTemplate {

  public String getMergeSql(String schemaName, String tableName, String[] pkNames, String[] colNames) {
    final String aliasA = "a";
    final String aliasB = "b";
    String[] allColumns = buildAllColumns(pkNames, colNames);

    StringBuilder sql = new StringBuilder();

    sql.append("merge /*+ use_nl(a b)*/ into ");
    sql.append(makeFullName(schemaName, tableName)).append(" ").append(aliasA);
    sql.append(" using (select ");

    int size = allColumns.length;
    // 构建 (select ? as col1, ? as col2 from dual)
    for (int i = 0; i < size; i++) {
      sql.append("? as " + allColumns[i]).append(splitCommea(size, i));
    }
    sql.append(" from dual) ").append(aliasB);
    sql.append(" on (");

    size = pkNames.length;
    for (int i = 0; i < size; i++) {
      sql.append(concat(pkNames[i], aliasA)).append("=").append(concat(pkNames[i], aliasB));
      sql.append((i + 1 < size) ? " and " : "");
    }

    sql.append(") when matched then update set ");

    size = colNames.length;
    for (int i = 0; i < size; i++) {
      sql.append(concat(colNames[i], aliasA)).append("=").append(concat(colNames[i], aliasB));
      sql.append(splitCommea(size, i));
    }

    sql.append(" when not matched then insert (");
    size = allColumns.length;
    for (int i = 0; i < size; i++) {
      sql.append(concat(allColumns[i], aliasA)).append(splitCommea(size, i));
    }

    sql.append(" ) values (");
    size = allColumns.length;
    for (int i = 0; i < size; i++) {
      sql.append(concat(allColumns[i], aliasB)).append(splitCommea(size, i));
    }
    sql.append(" )");
    // intern优化，避免出现大量相同的字符串
    return sql.toString().intern();
  }

  private String concat(String column, final String aliasA) {
    return aliasA + "." + column;
  }
}
