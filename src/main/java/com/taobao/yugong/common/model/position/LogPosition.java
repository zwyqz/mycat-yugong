package com.taobao.yugong.common.model.position;

/**
 * 增量日志文件的标示
 *
 * @author agapple 2012-6-14 下午09:20:07
 */
public class LogPosition extends Position {

  private static final long serialVersionUID = 81432665066427482L;

  private String journalName;
  private Long position;
  private Long timestamp;

  public LogPosition(String journalName, Long position, Long timestamp) {
    this.journalName = journalName;
    this.position = position;
    this.timestamp = timestamp;
  }

  public LogPosition(String journalName, Long position) {
    this.journalName = journalName;
    this.position = position;
  }

  public LogPosition(Long timestamp) {
    this.timestamp = timestamp;
  }

  public String getJournalName() {
    return journalName;
  }

  public void setJournalName(String journalName) {
    this.journalName = journalName;
  }

  public Long getPosition() {
    return position;
  }

  public void setPosition(Long position) {
    this.position = position;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public LogPosition clone() {
    return new LogPosition(journalName, position, timestamp);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((journalName == null) ? 0 : journalName.hashCode());
    result = prime * result + ((position == null) ? 0 : position.hashCode());
    result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    LogPosition other = (LogPosition) obj;
    if (journalName == null) {
      if (other.journalName != null) return false;
    } else if (!journalName.equals(other.journalName)) return false;
    if (position == null) {
      if (other.position != null) return false;
    } else if (!position.equals(other.position)) return false;
    if (timestamp == null) {
      if (other.timestamp != null) return false;
    } else if (!timestamp.equals(other.timestamp)) return false;
    return true;
  }

}
