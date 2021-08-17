package com.taobao.yugong.common.alarm;

import org.apache.commons.lang.StringUtils;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 发送邮件进行报警
 *
 * @author agapple 2013-9-6 上午11:42:04
 * @since 1.0.0
 */
public class MailAlarmService extends AbstractAlarmService {

  private static final String TITLE = "alarm_from_yugong";
  private JavaMailSender mailSender;
  private String emailUsername;
  private String emailPassword;
  private String emailHost;
  private int stmpPort = 465;
  private boolean sslSupport = true;

  @Override
  public void start() {
    super.start();
    JavaMailSenderImpl mailSender = new JavaMailSenderImpl();
    mailSender.setUsername(emailUsername);
    mailSender.setPassword(emailPassword);
    mailSender.setHost(emailHost);
    mailSender.setDefaultEncoding("UTF-8");
    Properties pros = new Properties();
    pros.put("mail.smtp.auth", true);
    pros.put("mail.smtp.timeout", 25000);
    pros.put("mail.smtp.port", stmpPort);
    pros.put("mail.smtp.socketFactory.port", stmpPort);
    pros.put("mail.smtp.socketFactory.fallback", false);
    if (sslSupport) {
      pros.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
    }

    mailSender.setJavaMailProperties(pros);

    this.mailSender = mailSender;
  }

  public void sendAlarm(AlarmMessage data) {
    SimpleMailMessage mail = new SimpleMailMessage(); // 只发送纯文本
    mail.setText(data.getMessage());// 邮件内容
    mail.setSubject(TITLE);// 主题
    mail.setFrom(emailUsername);

    String receiveKeys[] = StringUtils.split(StringUtils.replace(data.getReceiveKey(), ";", ","), ",");
    List<String> address = new ArrayList<String>();
    for (String receiveKey : receiveKeys) {
      if (isEmailAddress(receiveKey)) {
        address.add(receiveKey);
      } else if (isMobileNumber(receiveKey)) {
        // do nothing
      }
    }

    if (address != null && !address.isEmpty()) {
      mail.setTo(address.toArray(new String[address.size()]));
      sendMail(mail);
    }
  }

  private void sendMail(SimpleMailMessage mail) {
    // 正确设置了账户/密码，才尝试发送邮件
    if (StringUtils.isNotEmpty(emailUsername) && StringUtils.isNotEmpty(emailPassword)) {
      mailSender.send(mail);
    }
  }

  public void setMailSender(JavaMailSender mailSender) {
    this.mailSender = mailSender;
  }

  public void setEmailHost(String emailHost) {
    this.emailHost = emailHost;
  }

  public void setEmailUsername(String emailUsername) {
    this.emailUsername = emailUsername;
  }

  public void setEmailPassword(String emailPassword) {
    this.emailPassword = emailPassword;
  }

  public void setStmpPort(int stmpPort) {
    this.stmpPort = stmpPort;
  }

  public void setSslSupport(boolean sslSupport) {
    this.sslSupport = sslSupport;
  }

}
