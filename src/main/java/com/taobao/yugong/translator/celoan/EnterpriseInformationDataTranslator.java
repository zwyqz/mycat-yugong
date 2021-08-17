package com.taobao.yugong.translator.celoan;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.translator.DictDataTranslator;
import com.taobao.yugong.translator.SourceBackTableDataTranslator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.DataSource;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Objects;

import static com.taobao.yugong.common.utils.CeloanUtil.checkValueNotNull;
import static com.taobao.yugong.common.utils.CeloanUtil.replaceItemValue;

/**
 * 测试类 回调测试
 *
 * @author wyzhang
 * @date 2020/8/18 16:58
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class EnterpriseInformationDataTranslator extends SourceBackTableDataTranslator {



  @Override
  public boolean translator(DataSource sourceDs, DataSource targetDs, Record record) {

    /** 省市区修改 */
    replaceSiteData(record, "province_name");
    replaceSiteData(record, "city_name");
    replaceSiteData(record, "area_name");

    /** 法人身份证头像文件Id */
    ColumnValue licence_file_id = record.getColumnByName("licence_file_id", true);
    replaceItemValue(targetDs, licence_file_id, 0);
    /** 红黑名单 */
    ColumnValue red_black_list = record.getColumnByName("red_black_list", true);
    if (checkValueNotNull(red_black_list)) {
      String value = red_black_list.getValue().toString();
      switch (value) {
        case "0":
          red_black_list.setValue("10");
          break;
        case "1":
          red_black_list.setValue("20");
          break;
        case "2":
          red_black_list.setValue("30");
          break;
        default:
          break;
      }
    }

    /** 法人认证标识 */
    ColumnValue face_verify = record.getColumnByName("face_verify", true);
    ColumnValue lep_verify = record.getColumnByName("lep_verify", true);
    if (checkValueNotNull(face_verify) && "2".equals(face_verify.getValue().toString())) {
      record.addColumn(ColumnValue.buildVarchar("legala_auth_flag", "1"));
    } else if (checkValueNotNull(lep_verify) && "2".equals(lep_verify.getValue().toString())) {
      record.addColumn(ColumnValue.buildVarchar("legala_auth_flag", "2"));
    } else {
      record.addColumn(ColumnValue.buildVarchar("legala_auth_flag", "0"));
    }
    record.removeColumnByName("face_verify", true);
    record.removeColumnByName("lep_verify", true);

    /** * 经办人认证标识 */
    ColumnValue card_verify = record.getColumnByName("card_verify", true);
    if (checkValueNotNull(card_verify) && "2".equals(card_verify.getValue().toString())) {
      record.addColumn(ColumnValue.buildVarchar("agent_auth_flag", "2"));
    } else {
      record.addColumn(ColumnValue.buildVarchar("agent_auth_flag", "0"));
    }
    record.removeColumnByName("card_verify", true);

    /** * 认证状态 */
    ColumnValue isauth = record.getColumnByName("isauth", true);
    ColumnValue stateEdit = record.getColumnByName("stateEdit", true);
    if (checkValueNotNull(isauth) && "1".equals(isauth.getValue().toString())) {
      record.addColumn(ColumnValue.buildVarchar("authentication_flag", "1"));
    } else if (checkValueNotNull(stateEdit) && "3".equals(stateEdit.getValue().toString())) {
      record.addColumn(ColumnValue.buildVarchar("authentication_flag", "1"));
    } else {
      record.addColumn(ColumnValue.buildVarchar("authentication_flag", "0"));
    }
    /**
     * * 2.0 待审核 0 审核未通过 1 审核通过 2 审核取消 3
     *
     * <p>1.0 审核状态 1待审 2审核通过 3审核不通过
     */
    record.removeColumnByName("isauth");
    if (checkValueNotNull(stateEdit)) {
      String value = stateEdit.getValue().toString();
      switch (value) {
        case "1":
          record.addColumn(ColumnValue.buildVarchar("examine_result", "0"));
          break;
        case "2":
          record.addColumn(ColumnValue.buildVarchar("examine_result", "1"));
          break;
        case "3":
          record.addColumn(ColumnValue.buildVarchar("examine_result", "2"));
          break;
        default:
          break;
      }
    }

    return super.translator(record);
  }


  /** 将省市编码转为名称 */
  private void replaceSiteData(Record record, String columnName) {
    ColumnValue columnValue = record.getColumnByName(columnName, true);
    if (checkValueNotNull(columnValue)) {
      String name = DictDataTranslator.getSourceName(columnValue.getValue().toString());
      if (null != name && !"".equals(name)) {
        columnValue.setValue(name);
      }
    }
  }
}
