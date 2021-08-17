package com.taobao.yugong.common.version;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PACKAGE)
public @interface YuGongVersionAnnotation {

  String date();

  String url();

  String branch();

  String version();

  String hexVersion();
}
