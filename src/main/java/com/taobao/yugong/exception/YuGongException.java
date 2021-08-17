package com.taobao.yugong.exception;

import org.apache.commons.lang.exception.NestableRuntimeException;

/**
 * @author agapple 2013-9-9 下午2:32:16
 * @since 3.0.0
 */
public class YuGongException extends NestableRuntimeException {

  private static final long serialVersionUID = -654893533794556357L;

  public YuGongException(String errorCode) {
    super(errorCode);
  }

  public YuGongException(String errorCode, Throwable cause) {
    super(errorCode, cause);
  }

  public YuGongException(String errorCode, String errorDesc) {
    super(errorCode + ":" + errorDesc);
  }

  public YuGongException(String errorCode, String errorDesc, Throwable cause) {
    super(errorCode + ":" + errorDesc, cause);
  }

  public YuGongException(Throwable cause) {
    super(cause);
  }

  // public Throwable fillInStackTrace() {
  // return this;
  // }

}
