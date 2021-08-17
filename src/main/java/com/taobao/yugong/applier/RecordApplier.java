package com.taobao.yugong.applier;

import com.taobao.yugong.common.lifecycle.YuGongLifeCycle;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.exception.YuGongException;

import java.util.List;

/**
 * 数据提交
 *
 * @author agapple 2013-9-9 下午5:57:19
 * @since 3.0.0
 */
public interface RecordApplier extends YuGongLifeCycle {

  public void apply(List<Record> records) throws YuGongException;

}
