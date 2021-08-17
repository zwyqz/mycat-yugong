package com.taobao.yugong.positioner;

import com.taobao.yugong.common.lifecycle.YuGongLifeCycle;
import com.taobao.yugong.common.model.position.Position;
import com.taobao.yugong.exception.YuGongException;

/**
 * 数据定位器，记录最后一次成功的位置，抽象接口，允许定义各种存储实现
 *
 * @author agapple 2013-9-22 下午3:19:30
 */
public interface RecordPositioner extends YuGongLifeCycle {

  Position getLatest();

  void persist(Position position) throws YuGongException;

}
