package com.taobao.yugong.common.model;

import com.taobao.yugong.common.utils.YuGongToStringStyle;

import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.Properties;

/**
 * 数据介质源信息描述
 *
 * @author agapple 2011-9-2 上午11:28:21
 */
public class DataSourceConfig implements Serializable {

  private static final long serialVersionUID = -7653632703273608373L;
  private String username;
  private String password;
  private String url;
  private DbType type;
  private String encode;
  private Properties properties = new Properties();

  public DataSourceConfig() {

  }

  public DataSourceConfig(String url, String username, String password, DbType type, Properties properties) {
    this.username = username;
    this.password = password;
    this.url = url;
    this.type = type;
    this.properties = properties;
  }

  public DataSourceConfig(String url, String username, String password, DbType type, String encode,
      Properties properties) {
    this.username = username;
    this.password = password;
    this.url = url;
    this.type = type;
    this.encode = encode;
    this.properties = properties;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public DbType getType() {
    return type;
  }

  public void setType(DbType type) {
    this.type = type;
  }

  public String getEncode() {
    return encode;
  }

  public void setEncode(String encode) {
    this.encode = encode;
  }

  public Properties getProperties() {
    return properties;
  }

  public void setProperties(Properties properties) {
    this.properties = properties;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((encode == null) ? 0 : encode.hashCode());
    result = prime * result + ((password == null) ? 0 : password.hashCode());
    result = prime * result + ((properties == null) ? 0 : properties.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    result = prime * result + ((url == null) ? 0 : url.hashCode());
    result = prime * result + ((username == null) ? 0 : username.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    DataSourceConfig other = (DataSourceConfig) obj;
    if (encode == null) {
      if (other.encode != null) return false;
    } else if (!encode.equals(other.encode)) return false;
    if (password == null) {
      if (other.password != null) return false;
    } else if (!password.equals(other.password)) return false;
    if (properties == null) {
      if (other.properties != null) return false;
    } else if (!properties.equals(other.properties)) return false;
    if (type != other.type) return false;
    if (url == null) {
      if (other.url != null) return false;
    } else if (!url.equals(other.url)) return false;
    if (username == null) {
      if (other.username != null) return false;
    } else if (!username.equals(other.username)) return false;
    return true;
  }

  public String toString() {
    return ToStringBuilder.reflectionToString(this, YuGongToStringStyle.DEFAULT_STYLE);
  }

}
