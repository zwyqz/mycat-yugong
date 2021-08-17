package com.taobao.yugong.translator.celoan;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.translator.SourceBackTableDataTranslator;

import javax.sql.DataSource;

import static com.taobao.yugong.common.utils.CeloanUtil.replaceItemValue;

/**
 * 测试类 回调测试
 *
 * @author wyzhang
 * @date 2020/8/18 16:58
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class ClientCompanyToEnterpriseLegalDataTranslator extends SourceBackTableDataTranslator {


  @Override
  public boolean translator(DataSource sourceDs, DataSource targetDs, Record record) {
    /** 法人身份证头像文件Id */
    ColumnValue urlaFileId = record.getColumnByName("urla_file_id", true);
    replaceItemValue(targetDs, urlaFileId, 1);
    /** 法人身份证国徽文件Id */
    ColumnValue urlb_file_id = record.getColumnByName("urlb_file_id", true);
    replaceItemValue(targetDs, urlb_file_id, 1);
    /** 法人人脸识别文件Id */
    ColumnValue face_auth_img = record.getColumnByName("face_auth_img", true);
    replaceItemValue(targetDs, face_auth_img, 1);
    /** 法人授权文件Id */
    ColumnValue authorize_file_id = record.getColumnByName("authorize_file_id", true);
    replaceItemValue(targetDs, authorize_file_id, 4);

    return super.translator(record);
  }
}
