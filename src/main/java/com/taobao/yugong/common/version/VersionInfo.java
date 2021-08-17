package com.taobao.yugong.common.version;

import org.apache.commons.lang.SystemUtils;

public class VersionInfo {

  private static Package myPackage;
  private static YuGongVersionAnnotation version;

  static {
    myPackage = YuGongVersionAnnotation.class.getPackage();
    version = myPackage.getAnnotation(YuGongVersionAnnotation.class);
  }

  static Package getPackage() {
    return myPackage;
  }

  public static String getDate() {
    return (version != null) ? version.date() : "Unknown";
  }

  public static String getUrl() {
    return (version != null) ? version.url() : "Unknown";
  }

  public static String getBranch() {
    return (version != null) ? version.branch() : "Unknown";
  }

  public static String getVersion() {
    return (version != null) ? version.version() : "Unknown";
  }

  public static String getHexVevision() {
    return (version != null) ? version.hexVersion() : "Unknown";
  }

  public static String getBuildVersion() {
    StringBuilder buf = new StringBuilder();

    buf.append(SystemUtils.LINE_SEPARATOR);
    buf.append("[YuGong Version Info]").append(SystemUtils.LINE_SEPARATOR);
    buf.append("[version ]").append(VersionInfo.getVersion()).append(SystemUtils.LINE_SEPARATOR);
    buf.append("[hexVeision]").append(VersionInfo.getHexVevision()).append(SystemUtils.LINE_SEPARATOR);
    buf.append("[date    ]").append(VersionInfo.getDate()).append(SystemUtils.LINE_SEPARATOR);
    buf.append("[branch  ]").append(VersionInfo.getBranch()).append(SystemUtils.LINE_SEPARATOR);
    buf.append("[url     ]").append(VersionInfo.getUrl()).append(SystemUtils.LINE_SEPARATOR);

    return buf.toString();
  }

}
