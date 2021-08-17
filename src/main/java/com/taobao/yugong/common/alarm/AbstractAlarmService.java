package com.taobao.yugong.common.alarm;

import com.taobao.yugong.common.lifecycle.AbstractYuGongLifeCycle;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 报警服务实现
 *
 * @author agapple 2011-11-3 上午11:12:16
 * @version 4.0.0
 */
public abstract class AbstractAlarmService extends AbstractYuGongLifeCycle implements AlarmService {

  private final static Pattern MOBILE_PATTERN = Pattern.compile("^((13[0-9])|(15[^4,\\D])|(18[0,5-9]))\\d{8}$");

  private final static Pattern EMAIL_PATTERN = Pattern.compile("^\\w+([-+.]\\w+)*@\\w+([-.]\\w+)*\\.\\w+([-.]\\w+)*$");

  protected boolean isMobileNumber(String mobiles) {
    Matcher m = MOBILE_PATTERN.matcher(mobiles);
    return m.matches();
  }

  protected boolean isEmailAddress(String email) {
    boolean isExist = false;
    Matcher m = EMAIL_PATTERN.matcher(email);
    boolean b = m.matches();
    if (b) {
      isExist = true;
    }
    return isExist;
  }

}
