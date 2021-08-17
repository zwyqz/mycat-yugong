package com.taobao.yugong.common.lifecycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基本实现
 *
 * @author agapple 2014年2月25日 下午11:38:06
 * @since 1.0.0
 */
public abstract class AbstractYuGongLifeCycle implements YuGongLifeCycle {

  protected final Logger logger = LoggerFactory.getLogger(this.getClass());
  protected volatile boolean running = false;                                   // 是否处于运行中

  public boolean isStart() {
    return running;
  }

  public void start() {
    if (running) {
      return;
    }

    running = true;
  }

  public void stop() {
    if (!running) {
      return;
    }

    running = false;
  }

  public void abort(String why, Throwable e) {
    logger.error("abort caused by " + why, e);
    stop();
  }

  public boolean isStop() {
    return !isStart();
  }

}
