package com.taobao.yugong.common.utils.thread;

import org.slf4j.Logger;

public class YuGongUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

  private Logger logger;

  public YuGongUncaughtExceptionHandler(Logger logger) {
    this.logger = logger;
  }

  public void uncaughtException(Thread t, Throwable e) {
    logger.error("uncaught exception", e);
  }
}
