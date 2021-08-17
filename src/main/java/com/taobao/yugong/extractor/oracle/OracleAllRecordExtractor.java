package com.taobao.yugong.extractor.oracle;

import com.taobao.yugong.common.model.ExtractStatus;
import com.taobao.yugong.common.model.ProgressStatus;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.position.IdPosition;
import com.taobao.yugong.common.model.position.Position;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.utils.YuGongUtils;
import com.taobao.yugong.exception.YuGongException;
import com.taobao.yugong.positioner.RecordPositioner;

import java.util.List;

/**
 * 支持ALL模式的oracle数据同步
 *
 * @author agapple 2013-9-26 下午4:20:49
 */
public class OracleAllRecordExtractor extends AbstractOracleRecordExtractor {

  private AbstractOracleRecordExtractor markExtractor; // 增量记录接口
  private AbstractOracleRecordExtractor fullExtractor; // 全量抽取接口
  private AbstractOracleRecordExtractor incExtractor;  // 增量抽取接口
  private IdPosition currentPostion;
  private YuGongContext context;
  private RecordPositioner positioner;

  public OracleAllRecordExtractor(YuGongContext context) {
    this.context = context;

  }

  public void start() {
    super.start();
    currentPostion = (IdPosition) context.getLastPosition();
    if (currentPostion == null) {
      currentPostion = new IdPosition();
    }

    boolean hasMark = true;
    if (!currentPostion.isInHistory(ProgressStatus.MARK)) {
      hasMark = false;
      currentPostion.setCurrentProgress(ProgressStatus.MARK);// 进入mark阶段
      context.setLastPosition(currentPostion);
      positioner.persist(currentPostion);
      markExtractor.start();
    }

    // 如果是full跑完后，切到ALL时
    if (!currentPostion.isInHistory(ProgressStatus.FULLING) || !hasMark) {
      if (fullExtractor.isStart()) {
        throw new YuGongException("fullExtractor should start after markIncPosition , pls check");
      }

      currentPostion.setCurrentProgress(ProgressStatus.FULLING);// 进入fulling阶段
      context.setLastPosition(currentPostion);
      positioner.persist(currentPostion);
      fullExtractor.start();
    }
  }

  public void stop() {
    super.stop();
    if (incExtractor.isStart()) {
      incExtractor.stop();
    }

    if (fullExtractor.isStart()) {
      fullExtractor.stop();
    }

    if (markExtractor.isStart()) {
      markExtractor.stop();
    }
  }

  public List<Record> extract() throws YuGongException {
    List<Record> result = null;
    if (fullExtractor.isStart()) {
      result = fullExtractor.extract();

      if (YuGongUtils.isEmpty(result) && fullExtractor.status() == ExtractStatus.TABLE_END) {
        logger.info("table [{}] full extractor is end , next auto start inc extractor", context.getTableMeta()
            .getFullName());
        if (incExtractor.isStart()) {
          throw new YuGongException("incExtractor should start after fullExtractor , pls check");
        }
        // 关闭全量
        fullExtractor.stop();
        currentPostion.setCurrentProgress(ProgressStatus.INCING);// 进入incing阶段
        // 启动增量
        context.setLastPosition(currentPostion);
        positioner.persist(currentPostion);
        incExtractor.start();
      } else {
        return result; // 直接返回
      }
    } else if (!incExtractor.isStart()) {
      currentPostion.setCurrentProgress(ProgressStatus.INCING);// 进入incing阶段
      // 启动增量
      context.setLastPosition(currentPostion);
      positioner.persist(currentPostion);
      incExtractor.start();
    }

    if (incExtractor.isStart()) {
      result = incExtractor.extract();
    }

    return result;
  }

  public Position ack(List<Record> records) throws YuGongException {
    if (incExtractor.isStart()) {
      return incExtractor.ack(records);
    } else if (fullExtractor.isStart()) {
      return fullExtractor.ack(records);
    } else {
      throw new YuGongException("extractor is stop");
    }
  }

  public ExtractStatus status() {
    if (incExtractor.isStart()) {
      return incExtractor.status();
    } else if (fullExtractor.isStart()) {
      return fullExtractor.status();
    } else {
      throw new YuGongException("extractor is stop");
    }
  }

  public void setMarkExtractor(AbstractOracleRecordExtractor markExtractor) {
    this.markExtractor = markExtractor;
  }

  public void setFullExtractor(AbstractOracleRecordExtractor fullExtractor) {
    this.fullExtractor = fullExtractor;
  }

  public void setIncExtractor(AbstractOracleRecordExtractor incExtractor) {
    this.incExtractor = incExtractor;
  }

  public void setPositioner(RecordPositioner positioner) {
    this.positioner = positioner;
  }

}
