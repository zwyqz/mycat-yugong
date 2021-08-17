package com.taobao.yugong.common.model;

/**
 * @author agapple 2013-9-3 下午2:34:22
 * @since 3.0.0
 */
public enum ExtractStatus {
  /**
   * 正常提取
   */
  NORMAL,
  /**
   * 追上
   */
  CATCH_UP,
  /**
   * 无变更
   */
  NO_UPDATE,
  /**
   * 处理结束
   */
  TABLE_END;
}
