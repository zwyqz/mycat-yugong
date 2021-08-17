package com.taobao.yugong.common.stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 统计每个批次的运行信息
 *
 * @author agapple 2014年2月25日 下午11:38:06
 * @since 1.0.0
 */
public class StatAggregation {

  private static final Logger logger = LoggerFactory.getLogger(StatAggregation.class);
  private static final String HISTOGRAM_FORMAT = "{总记录数:%s,采样记录数:%s,同步TPS:%s,最长时间:%s,最小时间:%s,平均时间:%s}";
  private int bufferSize = 16 * 1024;
  private int indexMask;
  private AggregationItem[] table;
  private AtomicLong sequence = new AtomicLong(-1);
  private AtomicLong total = new AtomicLong(0);
  private int printInterval;                                                           // 打印频率

  public StatAggregation(int bufferSize, int printInterval) {
    if (Integer.bitCount(bufferSize) != 1) {
      throw new IllegalArgumentException("bufferSize must be a power of 2");
    }
    this.bufferSize = bufferSize;
    this.printInterval = printInterval;
    indexMask = this.bufferSize - 1;
    table = new AggregationItem[this.bufferSize];
  }

  public void push(AggregationItem aggregation) {
    long seq = sequence.incrementAndGet();
    table[getIndex(seq)] = aggregation;
    total.addAndGet(aggregation.getSize());

    if ((seq + 1) % printInterval == 0) { // 达到指定的输出频率
      printInterval(true);
    }
  }

  public void print() {
    printInterval(false);
  }

  public void printInterval(boolean isInterval) {
    String message = histogram(isInterval);
    logger.info("{}", message);
  }

  /**
   * 返回当前stage处理次数
   */
  public Long count() {
    return sequence.get();
  }

  /**
   * 平均处理时间
   */
  public String histogram(boolean isInterval) {
    Long costs = 0L;
    Long items = 0L;
    Long max = 0L;
    Long min = Long.MAX_VALUE;
    Long avg = 0L;
    Long numbers = 0L;
    Long tps = 0L;
    long end = 0;
    if (isInterval) {
      end = sequence.get() - printInterval + 1;
    } else {
      end = sequence.get() - bufferSize + 1;
    }

    if (end < 0) {
      end = 0;
    }
    for (long i = sequence.get(); i >= end; i--) {
      AggregationItem aggregation = table[getIndex(i)];
      if (aggregation != null) {
        Long cost = aggregation.getEndMillSeconds() - aggregation.getStartMillSeconds();
        items += 1;
        costs += cost;
        if (cost > max) {
          max = cost;
        }
        if (cost < min) {
          min = cost;
        }

        numbers += aggregation.getSize();
      }
    }

    if (items != 0) {
      avg = costs / items;
    }

    if (min == Long.MAX_VALUE) {
      min = 0L;
    }

    if (costs != 0) {
      tps = (numbers * 1000) / costs;
    }

    if (min != Long.MIN_VALUE) {
      return String.format(HISTOGRAM_FORMAT, new Object[]{total.get(), numbers, tps, max, min, avg});
    } else {
      return String.format(HISTOGRAM_FORMAT, new Object[]{total.get(), numbers, tps, max, 0, avg});
    }
  }

  private int getIndex(long sequcnce) {
    return (int) sequcnce & indexMask;
  }

  public static class AggregationItem {

    private Long startMillSeconds;
    private Long endMillSeconds;
    private Long size;

    public AggregationItem(Long startMillSeconds, Long endMillSeconds, Long size) {
      this.startMillSeconds = startMillSeconds;
      this.endMillSeconds = endMillSeconds;
      this.size = size;
    }

    public AggregationItem(Long startMillSeconds, Long endMillSeconds) {
      this.startMillSeconds = startMillSeconds;
      this.endMillSeconds = endMillSeconds;
    }

    public Long getStartMillSeconds() {
      return startMillSeconds;
    }

    public void setStartMillSeconds(Long startMillSeconds) {
      this.startMillSeconds = startMillSeconds;
    }

    public Long getEndMillSeconds() {
      return endMillSeconds;
    }

    public void setEndMillSeconds(Long endMillSeconds) {
      this.endMillSeconds = endMillSeconds;
    }

    public Long getSize() {
      return size;
    }

    public void setSize(Long size) {
      this.size = size;
    }

  }

  public void setPrintInterval(int printInterval) {
    this.printInterval = printInterval;
  }

}
