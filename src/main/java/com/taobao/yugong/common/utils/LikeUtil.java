package com.taobao.yugong.common.utils;

import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import com.google.common.collect.MigrateMap;
import com.taobao.yugong.exception.YuGongException;

import org.apache.commons.lang.StringUtils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LikeUtil {

  public static String[] regexSpecialChars = {"<", ">", "^", "$", "\\", "/", ";", "(", ")", "?", ".",
      "*", "[", "]", "+", "|"};
  public static String escapse = "\\";

  private static Map<String, Pattern> patterns = MigrateMap.makeComputingMap(new MapMaker(),
      new Function<String, Pattern>() {

        public Pattern apply(String pattern) {
          try {
            return Pattern.compile(buildPattern(pattern,
                escapse),
                Pattern.CASE_INSENSITIVE);
          } catch (Throwable e) {
            throw new YuGongException(e);
          }
        }
      });

  public static boolean isMatch(String pattern, String value) {
    if (StringUtils.equalsIgnoreCase(pattern, value)) {
      return true;
    } else if (StringUtils.isEmpty(value)) {
      return false;
    } else if (StringUtils.isEmpty(pattern)) {
      return false;
    }

    Pattern pat = patterns.get(pattern);
    Matcher m = pat.matcher(value);
    return m.matches();
  }

  private static String buildPattern(String pattern, String escape) {
    char esc = escape.charAt(0);
    pattern = StringUtils.trim(pattern);
    StringBuilder builder = new StringBuilder("^");
    int index = 0, last = 0;
    while (true) {
      // 查找esc
      index = pattern.indexOf(esc, last);
      if (index == -1 || index >= pattern.length()) {
        if (last < pattern.length()) {
          builder.append(convertWildcard(ripRegex(pattern.substring(last))));
        }
        break;
      }
      if (index > 0) {
        String toRipRegex = StringUtils.substring(pattern, last, index);
        builder.append(convertWildcard(ripRegex(toRipRegex)));
        last = index;
      }
      if (index + 1 < pattern.length()) {
        builder.append(ripRegex(pattern.charAt(index + 1)));
        last = index + 2;
      } else {
        builder.append(pattern.charAt(index));
        last = index + 1;
      }
      if (last >= pattern.length()) {
        break;
      }
    }
    builder.append('$');
    return builder.toString();
  }

  private static String ripRegex(char toRip) {
    char[] chars = new char[1];
    chars[0] = toRip;
    return ripRegex(new String(chars));
  }

  private static String ripRegex(String toRip) {
    for (String c : regexSpecialChars) {
      toRip = StringUtils.replace(toRip, c, "\\" + c);
    }

    return toRip;
  }

  private static String convertWildcard(String toConvert) {
    toConvert = StringUtils.replace(toConvert, "_", "(.)");
    toConvert = StringUtils.replace(toConvert, "%", "(.*)");
    return toConvert;
  }
}
