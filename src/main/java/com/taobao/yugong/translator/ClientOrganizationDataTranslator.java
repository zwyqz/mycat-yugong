package com.taobao.yugong.translator;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
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
public class ClientOrganizationDataTranslator extends SourceBackTableDataTranslator {
  public static final long CURRENT_TIME = 1599719869901L;

//  public String getString(DataSource ds, String sql ) {
//    JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
////    return (String)jdbcTemplate.queryForObject(sql, String.class);
//    return (String)jdbcTemplate.query(
//            sql,
//            new ResultSetExtractor() {
//              public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
//                if (rs.next()) {
//                  return rs.getString(1);
//                }
//                return null;
//              }
//            });
//
//  }  public String getString(DataSource ds, String sql ) {
//    JdbcTemplate jdbcTemplate = new JdbcTemplate(ds);
////    return (String)jdbcTemplate.queryForObject(sql, String.class);
//    return (String)jdbcTemplate.query(
//            sql,
//            new ResultSetExtractor() {
//              public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
//                if (rs.next()) {
//                  return rs.getString(1);
//                }
//                return null;
//              }
//            });
//
//  }
  public String getDictValue(DataSource sourceDs, DataSource targetDs, Record record, String key) {

    if(record.getColumnByName(key, true) == null) {
      record.addColumn(ColumnValue.buildVarchar(key, null));
      record.addColumn(ColumnValue.buildVarchar(key + "_code",  null));
      return "";
    }

    if(record.getColumnByName(key).getValue() == null  || StringUtils.isEmpty(record.getColumnByName(key).getValue().toString())) {
      ColumnMeta codeMeta = new ColumnMeta(key + "_code", Types.VARCHAR);
      ColumnValue codeColumn = new ColumnValue(codeMeta, "");
      record.addColumn(codeColumn);
      return "";
    }

    String code = record.getColumnByName(key).getValue().toString();
    String name = DictDataTranslator.getSourceName(code);

    String dict_value = DictDataTranslator.getTargetKey(name);

    record.getColumnByName(key).setValue(name);

    ColumnMeta codeMeta = new ColumnMeta(key + "_code", Types.VARCHAR);
    ColumnValue codeColumn = new ColumnValue(codeMeta, dict_value);
    record.addColumn(codeColumn);
   return "";
  }


  @Override
  public boolean translator(DataSource sourceDs, DataSource targetDs, Record record) {


     getDictValue(sourceDs, targetDs, record, "province");
     getDictValue(sourceDs, targetDs, record, "city");
     getDictValue(sourceDs, targetDs, record, "district");

    ColumnValue createTime = record.getColumnByName("create_time");
    if (createTime.getValue() == null) {
      createTime.setValue(new Date(CURRENT_TIME));
    }

    ColumnValue introduction = record.getColumnByName("introduction");
    if (introduction.getValue() == null) {
      introduction.setValue("");
    }

    ColumnValue logo = record.getColumnByName("logo");
    if (logo.getValue() == null) {
      logo.setValue("");
    }

    ColumnValue address = record.getColumnByName("address");
    if (address.getValue() == null) {
      address.setValue("");
    }

    return super.translator(record);
  }
}
