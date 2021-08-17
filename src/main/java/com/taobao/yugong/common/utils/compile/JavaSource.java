package com.taobao.yugong.common.utils.compile;

import com.google.common.base.Function;
import com.google.common.collect.MigrateMap;

import org.apache.commons.lang.StringUtils;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;

import java.util.Map;

/**
 * @author agapple 2016年2月25日 下午10:49:10
 * @since 1.0.0
 */
public class JavaSource {

  private static Map<String, Pattern> patterns = null;

  static {
    patterns = MigrateMap.makeComputingMap(new Function<String, Pattern>() {

      public Pattern apply(String pattern) {
        try {
          PatternCompiler pc = new Perl5Compiler();
          return pc.compile(pattern, Perl5Compiler.CASE_INSENSITIVE_MASK | Perl5Compiler.READ_ONLY_MASK);
        } catch (MalformedPatternException e) {
          throw new RuntimeException("Regex failed!", e);
        }
      }
    });
  }

  private String packageName;
  private String className;
  private String source;

  public JavaSource(String sourceString) {
    String className = findFirst(sourceString, "public class (?s).*?{").split("extends")[0].split("implements")[0].replaceAll("public class ",
        StringUtils.EMPTY)
        .replace("{", StringUtils.EMPTY)
        .trim();
    String packageName = findFirst(sourceString, "package (?s).*?;").replaceAll("package ", StringUtils.EMPTY)
        .replaceAll(";", StringUtils.EMPTY)
        .trim();
    this.packageName = packageName;
    this.className = className;
    this.source = sourceString;
  }

  public JavaSource(String packageName, String className, String source) {
    this.packageName = packageName;
    this.className = className;
    this.source = source;
  }

  public String getPackageName() {
    return packageName;
  }

  public void setPackageName(String packageName) {
    this.packageName = packageName;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public String getFullName() {
    return packageName + "." + className;
  }

  public String toString() {
    return getFullName();
  }

  public String findFirst(String originalStr, String regex) {
    if (StringUtils.isBlank(originalStr) || StringUtils.isBlank(regex)) {
      return StringUtils.EMPTY;
    }

    PatternMatcher matcher = new Perl5Matcher();
    if (matcher.contains(originalStr, patterns.get(regex))) {
      return StringUtils.trimToEmpty(matcher.getMatch().group(0));
    }
    return StringUtils.EMPTY;
  }
}
