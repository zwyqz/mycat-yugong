package com.taobao.yugong.common.stats;

import com.taobao.yugong.common.model.ProgressStatus;
import com.taobao.yugong.common.model.RunMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 统计下当前各表迁移的状态
 *
 * @author agapple 2014-4-24 下午2:12:13
 * @since 3.0.4
 */
public class ProgressTracer {

  private static final Logger logger = LoggerFactory.getLogger(ProgressTracer.class);
  private static final String FULL_FORMAT = "{未启动:%s,全量中:%s,已完成:%s,异常数:%s}";
  private static final String INC_FORMAT = "{未启动:%s,增量中:%s,已追上:%s,异常数:%s}";
  private static final String CHECK_FORMAT = "{未启动:%s,对比中:%s,已完成:%s,异常数:%s}";
  private static final String ALL_FORMAT = "{未启动:%s,全量中:%s,增量中:%s,已追上:%s,异常数:%s}";

  private int total;
  private RunMode mode;
  private Map<String, ProgressStatus> status = new ConcurrentHashMap<String, ProgressStatus>();

  public ProgressTracer(RunMode mode, int total) {
    this.mode = mode;
    this.total = total;
  }

  public void update(String tableName, ProgressStatus progress) {
    ProgressStatus st = status.get(tableName);
    if (st != ProgressStatus.FAILED) {
      status.put(tableName, progress);
    }
  }

  public void printSummry() {
    print(false);
  }

  public void print(boolean detail) {
    int fulling = 0;
    int incing = 0;
    int failed = 0;
    int success = 0;
    List<String> fullingTables = new ArrayList<String>();
    List<String> incingTables = new ArrayList<String>();
    List<String> failedTables = new ArrayList<String>();
    List<String> successTables = new ArrayList<String>();

    for (Map.Entry<String, ProgressStatus> entry : status.entrySet()) {
      ProgressStatus progress = entry.getValue();
      if (progress == ProgressStatus.FULLING) {
        fulling++;
        fullingTables.add(entry.getKey());
      } else if (progress == ProgressStatus.INCING) {
        incing++;
        incingTables.add(entry.getKey());
      } else if (progress == ProgressStatus.FAILED) {
        failed++;
        failedTables.add(entry.getKey());
      } else if (progress == ProgressStatus.SUCCESS) {
        success++;
        successTables.add(entry.getKey());
      }
    }

    int unknow = this.total - fulling - incing - failed - success;
    String msg = null;
    if (mode == RunMode.ALL) {
      msg = String.format(ALL_FORMAT, unknow, fulling, incing, success, failed);
    } else if (mode == RunMode.FULL) {
      msg = String.format(FULL_FORMAT, unknow, fulling, success, failed);
    } else if (mode == RunMode.INC) {
      msg = String.format(INC_FORMAT, unknow, incing, success, failed);
    } else if (mode == RunMode.CHECK) {
      msg = String.format(CHECK_FORMAT, unknow, fulling, success, failed);
    }

    logger.info("{}", msg);
    if (detail) {
      if (fulling > 0) {
        if (mode == RunMode.CHECK) {
          logger.info("对比中:" + fullingTables);
        } else {
          logger.info("全量中:" + fullingTables);
        }
      }
      if (incing > 0) {
        logger.info("增量中:" + incingTables);
      }
      if (failed > 0) {
        logger.info("异常数:" + failedTables);
      }
      logger.info("已完成:" + successTables);
    }
  }
}
