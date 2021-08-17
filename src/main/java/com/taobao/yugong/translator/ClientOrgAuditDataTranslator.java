package com.taobao.yugong.translator;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;

import javax.sql.DataSource;

/**
 * 测试类 回调测试
 *
 * @author wyzhang
 * @date 2020/8/18 16:58
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class ClientOrgAuditDataTranslator extends SourceBackTableDataTranslator {

  @Override
  public boolean translator(DataSource sourceDs, DataSource targetDs, Record record) {

//    ColumnValue createTime = record.getColumnByName("create_time");
//    if (createTime.getValue() == null) {
//      createTime.setValue(new Date(CURRENT_TIME));
//    }
      ColumnValue auditDesc = record.getColumnByName("audit_desc");
    if (auditDesc.getValue() == null) {
      auditDesc.setValue("");
    }
    return super.translator(record);
  }
}
