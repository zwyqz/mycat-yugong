package com.taobao.yugong.common.utils;

import org.apache.commons.lang.StringUtils;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

public class ConsoleFilter extends Filter<ILoggingEvent> {

  private static boolean isEclipse = false;

  static {
    // 脚本启动时,一定会带上appName
    String appname = System.getProperty("appName");
    isEclipse = StringUtils.isEmpty(appname);
  }

  @Override
  public FilterReply decide(ILoggingEvent event) {
    if (isEclipse) {
      // 只有在eclipse启动时才输出
      return FilterReply.ACCEPT;
    } else {
      return FilterReply.DENY;
    }
  }
}
