package com.taobao.yugong.translator.celoan;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.translator.SourceBackTableDataTranslator;

import javax.sql.DataSource;

import static com.taobao.yugong.common.utils.CeloanUtil.checkValueNotNull;
import static com.taobao.yugong.common.utils.CeloanUtil.getSingle;

/**
 *
 * ec_enterprise_financing_info:企业融资需求表
 * @author wyzhang
 * @date 2020/8/18 16:58
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class EnterpriseFinancingInfoDataTranslator extends SourceBackTableDataTranslator {


  @Override
  public boolean translator(DataSource sourceDs, DataSource targetDs, Record record) {

    /**还款方式*/
    ColumnValue repayment_type = record.getColumnByName("repayment_type", true);
    if (checkValueNotNull(repayment_type)) {
      String value = repayment_type.getValue().toString();
      switch (value) {
        case "1":
          repayment_type.setValue("4");
          break;
        case "2":
          repayment_type.setValue("3");
          break;
        case "3":
          repayment_type.setValue("7");
          break;
        case "4":
          repayment_type.setValue("2");
          break;
        case "5":
          repayment_type.setValue("5");
          break;
        case "6":
          repayment_type.setValue("6");
          break;
        case "7":
          repayment_type.setValue("1");
          break;
        default:
          break;
      }
    }

    /**到账时间类型*/
    ColumnValue arrival_time_type = record.getColumnByName("arrival_time_type", true);
    if(checkValueNotNull(arrival_time_type)) {
        Integer value = Integer.valueOf(arrival_time_type.getValue().toString());
        if(value <= 7 ) {
          arrival_time_type.setValue(1);
        } else if (value <= 14) {
          arrival_time_type.setValue(2);
        } else  if(value <= 30) {
          arrival_time_type.setValue(3);
        } else {
          arrival_time_type.setValue(4);
        }
    }

    /**盈亏状态选项*/

    /**纳税等级选项 */
    ColumnValue tax_amount = record.getColumnByName("tax_amount", true);
    if(checkValueNotNull(tax_amount)) {
      String value = tax_amount.getValue().toString();
      switch (value) {
        case "A":
          tax_amount.setValue("1");
          break;
        case "B":
          tax_amount.setValue("2");
          break;
        case "C":
          tax_amount.setValue("3");
          break;
        case "D":
          tax_amount.setValue("4");
          break;
        default:
          break;
      }
    }
    /** 融资顾问*/
    record.addColumn(ColumnValue.buildVarchar("financial_advisor",
            findAdvisor(sourceDs, record.getColumnByName("advisor", true))));

    /** 融资顾问手机号*/
    record.addColumn(ColumnValue.buildVarchar("financial_advisor_tell",
            findAdvisorTell(sourceDs, record.getColumnByName("advisor", true))));
    record.removeColumnByName("advisor", true);

    /***备注【融资取消理由其他】*/
    record.addColumn(ColumnValue.buildVarchar("remarks",
            findRemarks(sourceDs, record.getColumnByName("id", true))));

    return super.translator(record);

  }

  private String findAdvisor(DataSource sourceDs, ColumnValue advisor) {
    if(checkValueNotNull(advisor)) {
      Integer value = Integer.valueOf( advisor.getValue().toString());
      String sql = String.format("select realname from t_base_users where id = %s" , value );
      return getSingle(sourceDs, sql);
    }
    return null;
  }
  private String findAdvisorTell(DataSource sourceDs, ColumnValue advisor) {
    if(checkValueNotNull(advisor)) {
      Integer value = Integer.valueOf( advisor.getValue().toString());
      String sql = String.format("select mobile from t_base_users where id = %s" , value );
      return getSingle(sourceDs, sql);
    }
    return null;
  }
  private String findRemarks(DataSource sourceDs, ColumnValue id) {
    if(checkValueNotNull(id)) {
      String sql = String.format("select remark from t_client_financing_node where node_state  = 30 and nid = %s" , id.getValue().toString());
      return getSingle(sourceDs, sql);
    }
    return null;
  }


}
