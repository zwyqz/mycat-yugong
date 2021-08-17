package com.taobao.yugong.translator;

import com.taobao.yugong.common.YuGongConstants;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.utils.thread.ExecutorTemplate;
import com.taobao.yugong.exception.YuGongException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.sql.DataSource;

/**
 * 允许业务进行回表查询
 *
 * @author agapple 2013-9-12 下午5:38:07
 */
public abstract class BackTableDataTranslator extends AbstractDataTranslator implements DataTranslator {

  /**
   * 提供源数据库和对应的record
   */
  public abstract boolean translator(DataSource sourceDs, Record record);

  public List<Record> translator(final DataSource sourceDs, final DataSource targetDs, final List<Record> records,
      final ExecutorTemplate template) {
    final List<Record> result = Collections.synchronizedList(new ArrayList<Record>());
    final String spiltKey = MDC.get(YuGongConstants.MDC_TABLE_SHIT_KEY);
    for (final Record record : records) {
      if (template != null) {
        template.submit(new Runnable() {

          public void run() {
            String name = Thread.currentThread().getName();
            try {
              MDC.put(YuGongConstants.MDC_TABLE_SHIT_KEY, spiltKey);
              Thread.currentThread().setName(this.getClass().getSimpleName()
                  + "-"
                  + StringUtils.join(Arrays.asList(record.getSchemaName(),
                  record.getTableName()), '.'));
              if (translator(sourceDs, record)) {
                result.add(record);
              }
            } catch (Throwable e) {
              throw new YuGongException("failed record data : " + record.toString(), e);
            } finally {
              Thread.currentThread().setName(name);
            }
          }
        });
      } else {
        if (translator(sourceDs, record)) {
          result.add(record);
        }
      }
    }

    if (template != null) {
      // 等待所有结果完成
      template.waitForResult();
    }
    return result;
  }

  public List<Record> translator(List<Record> records) {
    throw new UnsupportedOperationException("BackTableDataTranslator Not Support this function");
  }
}
