package com.taobao.yugong.extractor;

import com.taobao.yugong.common.lifecycle.YuGongLifeCycle;
import com.taobao.yugong.common.model.ExtractStatus;
import com.taobao.yugong.common.model.position.Position;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.exception.YuGongException;

import java.util.List;

/**
 * 数据获取
 *
 * @author agapple 2013-9-3 下午2:36:56
 * @since 3.0.0
 */
public interface RecordExtractor extends YuGongLifeCycle {

  /**
   * 获取增量数据
   */
  public List<Record> extract() throws YuGongException;

  /**
   * @return 当前extractor的状态,{@linkplain ExtractStatus}
   */
  public ExtractStatus status();

  /**
   * 反馈数据处理成功
   */
  public Position ack(List<Record> records) throws YuGongException;
}
