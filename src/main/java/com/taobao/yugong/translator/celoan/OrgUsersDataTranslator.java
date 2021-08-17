package com.taobao.yugong.translator.celoan;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.translator.DictDataTranslator;
import com.taobao.yugong.translator.SourceBackTableDataTranslator;
import org.apache.commons.lang.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import javax.sql.DataSource;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * 测试类 回调测试
 *
 * @author wyzhang
 * @date 2020/8/18 16:58
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class OrgUsersDataTranslator extends SourceBackTableDataTranslator {

  public static final String PROVINCE = "province";
  public static final String CITY = "city";
  public static final String NAME = "county";

  @Override
  public boolean translator(DataSource sourceDs, DataSource targetDs, Record record) {

    String orgId = record.getColumnByName("orgid").getValue().toString();

    JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceDs);
    jdbcTemplate.query(
        String.format(
            "select province, city, area from  t_org_organization where id = %s\n", orgId),
        new ResultSetExtractor() {
          public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
            if (rs.next()) {
              record.addColumn(ColumnValue.buildVarchar(PROVINCE, rs.getString(PROVINCE)));
              record.addColumn(ColumnValue.buildVarchar(CITY, rs.getString(CITY)));
              record.addColumn(ColumnValue.buildVarchar(NAME, rs.getString("area")));
              return "";
            }
            return null;
          }
        });
    record.removeColumnByName("orgid");
    return super.translator(record);
  }
}
