package com.taobao.yugong.common.alarm;

/**
 * 报警服务service定义,暂时先简单实现：利用dragoon的报警推送机制进行短信，邮件，旺旺信息等报警
 *
 * @author agapple 2011-9-26 下午10:27:44
 * @version 4.0.0
 */
public interface AlarmService {

  /**
   * 发送基于kv的报警信息
   *
   * <pre>
   * Map内容；
   * 1. message : 报警内容
   * 2. receiveKey : 报警接收者信息
   * </pre>
   */
  public void sendAlarm(AlarmMessage data);

}
