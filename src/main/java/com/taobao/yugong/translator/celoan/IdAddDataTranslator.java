package com.taobao.yugong.translator.celoan;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.translator.DictDataTranslator;
import com.taobao.yugong.translator.SourceBackTableDataTranslator;
import lombok.Setter;
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
public class IdAddDataTranslator extends SourceBackTableDataTranslator {

  @Setter
  private String name;

  private Integer idAddPrefix;
  public void setIdAddPrefix(Integer idAddPrefix) {
    this.idAddPrefix = idAddPrefix;
  }


  @Override
  public boolean translator(DataSource sourceDs, DataSource targetDs, Record record) {



    /**id加固定值*/
    ColumnValue idMeta = record.getColumnByName(name);

    if( idMeta  == null || idMeta.getValue() == null || "".equals(idMeta.getValue())) {
      return super.translator(record);
    }

    idMeta.setValue(Integer.valueOf(idMeta.getValue().toString()) + idAddPrefix);
    return super.translator(record);
  }
}
