package com.taobao.yugong.translator.core;

import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.utils.YuGongUtils;
import com.taobao.yugong.translator.AbstractDataTranslator;

/**
 * 处理下编码转化
 *
 * @author agapple 2013-9-29 下午3:24:19
 */
public class EncodeDataTranslator extends AbstractDataTranslator {

  private String sourceEncoding;
  private String targetEncoding;

  public EncodeDataTranslator(String sourceEncoding, String targetEncoding) {
    this.sourceEncoding = sourceEncoding;
    this.targetEncoding = targetEncoding;
  }

  public boolean translator(Record record) {
    for (ColumnValue pk : record.getPrimaryKeys()) {
      pk.setValue(YuGongUtils.encoding(pk.getValue(), pk.getColumn().getType(), sourceEncoding, targetEncoding));
    }

    for (ColumnValue column : record.getColumns()) {
      column.setValue(YuGongUtils.encoding(column.getValue(),
          column.getColumn().getType(),
          sourceEncoding,
          targetEncoding));
    }

    return super.translator(record);
  }
}
