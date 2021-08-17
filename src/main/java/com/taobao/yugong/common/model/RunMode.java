package com.taobao.yugong.common.model;

/**
 * 运行模式
 *
 * @author agapple 2013-9-17 下午5:22:06
 */
public enum RunMode {

  /**
   * 增量记录
   */
  MARK,
  /**
   * 增量
   */
  INC,
  /**
   * 全量
   */
  FULL,
  /**
   * full+inc自动运行
   */
  ALL,
  /**
   * 对比
   */
  CHECK,
  /**
   * 清理
   */
  CLEAR;

  /**
   * 增量记录
   */
  public boolean isMark() {
    return this == RunMode.MARK;
  }

  /**
   * 增量清理
   */
  public boolean isClear() {
    return this == RunMode.CLEAR;
  }

  /**
   * 增量
   */
  public boolean isInc() {
    return this == RunMode.INC;
  }

  /**
   * 全量
   */
  public boolean isFull() {
    return this == RunMode.FULL;
  }

  /**
   * full+inc自动运行
   */
  public boolean isAll() {
    return this == RunMode.ALL;
  }

  public boolean isCheck() {
    return this == RunMode.CHECK;
  }
}
