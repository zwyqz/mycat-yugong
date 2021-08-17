package com.taobao.yugong.positioner;

import com.taobao.yugong.common.lifecycle.AbstractYuGongLifeCycle;
import com.taobao.yugong.common.model.position.Position;
import com.taobao.yugong.exception.YuGongException;

/**
 * 简单的内存记录
 *
 * @author agapple 2013-9-22 下午3:35:32
 */
public class MemoryRecordPositioner extends AbstractYuGongLifeCycle implements RecordPositioner {

  protected volatile Position position;

  public Position getLatest() {
    return position;
  }

  public void persist(Position position) throws YuGongException {
    this.position = position;
  }

}
