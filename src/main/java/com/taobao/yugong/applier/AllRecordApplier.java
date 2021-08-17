package com.taobao.yugong.applier;

import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.record.IncrementRecord;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.utils.YuGongUtils;
import com.taobao.yugong.exception.YuGongException;

import java.util.List;

/**
 * 全量+增量的applier实现
 *
 * @author agapple 2013-9-26 下午4:43:20
 */
public class AllRecordApplier extends AbstractRecordApplier {

  private RecordApplier fullApplier;
  private RecordApplier incApplier;
  @SuppressWarnings("unused")
  private YuGongContext context;

  public AllRecordApplier(YuGongContext context) {
    this.context = context;
  }

  public void start() {
    super.start();

    if (!fullApplier.isStart()) {
      fullApplier.start();
    }

    // inc启动基本不占资源，一起启动了，省的每次判断
    if (!incApplier.isStart()) {
      incApplier.start();
    }
  }

  public void stop() {
    super.stop();

    if (fullApplier.isStart()) {
      fullApplier.stop();
    }

    if (incApplier.isStart()) {
      incApplier.stop();
    }
  }

  public void apply(List<Record> records) throws YuGongException {
    if (YuGongUtils.isEmpty(records)) {
      return;
    }

    if (!isInc(records)) {
      fullApplier.apply(records);
    } else {
      incApplier.apply(records);
    }
  }

  /**
   * 不会出现数据一半是全量，一半是增量的数据,只有一种类型的数据
   */
  private boolean isInc(List<Record> records) {
    return records.get(0) instanceof IncrementRecord;
  }

  public void setFullApplier(RecordApplier fullApplier) {
    this.fullApplier = fullApplier;
  }

  public void setIncApplier(RecordApplier incApplier) {
    this.incApplier = incApplier;
  }

}
