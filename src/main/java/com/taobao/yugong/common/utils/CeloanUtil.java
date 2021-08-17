package com.taobao.yugong.common.utils;

import com.taobao.yugong.common.db.meta.ColumnValue;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Objects;

/**
 * 类说明
 *
 * @author wyzhang
 * @date 2020/9/15 14:51
 */
public class CeloanUtil {
  public static boolean checkValueNotNull(ColumnValue columnValue) {
    if (columnValue == null
        || columnValue.getValue() == null
        || "".equals(columnValue.getValue().toString())) {
      return false;
    }
    return true;
  }

  /**
   * *
   *
   * <p>委托书模板 0 营业执照 1 法人身份证 2 经办人身份证 3 委托授权书 4
   *
   * @param targetDs
   * @param value
   * @return
   */
  public static void replaceItemValue(DataSource targetDs, ColumnValue value, Integer fileType) {
    if (value == null || value.getValue() == null || "".equals(value.getValue())) {
      return;
    }
    JdbcTemplate jdbcTemplate = new JdbcTemplate(targetDs);

    String sql = "insert into ec_enterprise_file ( file_type, file_url, create_time) value(?,?,?)";
    // 获取插入数据的自增主键
    KeyHolder holder = new GeneratedKeyHolder();
    jdbcTemplate.update(
        connection -> {
          PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
          ps.setInt(1, fileType);
          ps.setString(2, value.getValue().toString());
          ps.setDate(3, new Date(System.currentTimeMillis()));
          return ps;
        },
        holder);

    value.setValue(Objects.requireNonNull(holder.getKey()).longValue());
  }

  public static String getSingle(DataSource ds, String sql) {
    JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
    return (String)
        jdbcTemplate.query(
            sql,
            new ResultSetExtractor() {
              @Override
              public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
                if (rs.next()) {
                  return rs.getString(1);
                }
                return null;
              }
            });
  }
}
