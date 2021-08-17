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
import java.util.Map;

/**
 * createTime字段过滤器
 *
 * @author wyzhang
 * @date 2020/8/18 16:58
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class CreateTimeTranslator extends SourceBackTableDataTranslator {
  private long createTimeLong;
  public void setCreateTime(long createTime) {
    this.createTimeLong = createTime;
  }

  @Override
  public boolean translator(DataSource sourceDs, DataSource targetDs, Record record) {

    ColumnValue createTime = record.getColumnByName("create_time");
    if (createTime.getValue() == null) {
      createTime.setValue(new Date(createTimeLong));
    }

    return super.translator(record);
  }
}
