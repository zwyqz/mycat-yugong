package com.taobao.yugong.extractor;

import com.taobao.yugong.common.lifecycle.AbstractYuGongLifeCycle;
import com.taobao.yugong.common.model.ExtractStatus;
import com.taobao.yugong.common.stats.ProgressTracer;

import lombok.Getter;
import lombok.Setter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author agapple 2014年2月25日 下午11:38:06
 * @since 1.0.0
 */

public abstract class AbstractRecordExtractor extends AbstractYuGongLifeCycle
    implements RecordExtractor {

  protected final Logger logger = LoggerFactory.getLogger(this.getClass());

  @Getter
  @Setter
  protected volatile ExtractStatus status = ExtractStatus.NORMAL;

  public ExtractStatus status() {
    return status;
  }

  @Getter
  @Setter
  protected volatile ProgressTracer tracer;
}
