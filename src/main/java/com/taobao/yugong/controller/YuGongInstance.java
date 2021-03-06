package com.taobao.yugong.controller;

import com.google.common.collect.Lists;
import com.taobao.yugong.applier.RecordApplier;
import com.taobao.yugong.common.YuGongConstants;
import com.taobao.yugong.common.alarm.AlarmMessage;
import com.taobao.yugong.common.alarm.AlarmService;
import com.taobao.yugong.common.audit.RecordDumper;
import com.taobao.yugong.common.lifecycle.AbstractYuGongLifeCycle;
import com.taobao.yugong.common.model.DbType;
import com.taobao.yugong.common.model.ExtractStatus;
import com.taobao.yugong.common.model.ProgressStatus;
import com.taobao.yugong.common.model.RunMode;
import com.taobao.yugong.common.model.YuGongContext;
import com.taobao.yugong.common.model.position.Position;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.common.stats.ProgressTracer;
import com.taobao.yugong.common.stats.StatAggregation;
import com.taobao.yugong.common.stats.StatAggregation.AggregationItem;
import com.taobao.yugong.common.utils.YuGongUtils;
import com.taobao.yugong.common.utils.thread.ExecutorTemplate;
import com.taobao.yugong.common.utils.thread.NamedThreadFactory;
import com.taobao.yugong.common.utils.thread.YuGongUncaughtExceptionHandler;
import com.taobao.yugong.exception.YuGongException;
import com.taobao.yugong.extractor.RecordExtractor;
import com.taobao.yugong.positioner.RecordPositioner;
import com.taobao.yugong.translator.BackTableDataTranslator;
import com.taobao.yugong.translator.DataTranslator;
import com.taobao.yugong.translator.TableMetaTranslator;
import com.taobao.yugong.translator.core.EncodeDataTranslator;
import com.taobao.yugong.translator.core.OracleIncreamentDataTranslator;

import lombok.Getter;
import lombok.Setter;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ??????????????????????????????
 *
 * @author agapple 2013-9-17 ??????3:21:01
 */
public class YuGongInstance extends AbstractYuGongLifeCycle {

  private final Logger logger = LoggerFactory.getLogger(YuGongInstance.class);
  @Getter
  @Setter
  private YuGongContext context;
  @Getter
  @Setter
  private RecordExtractor extractor;
  @Getter
  @Setter
  private RecordApplier applier;
  @Getter
  @Setter
  private List<DataTranslator> defaultTranslators = Lists.newArrayList();
  @Getter
  @Setter
  private List<DataTranslator> translators = Lists.newArrayList();
  @Getter
  @Setter
  private List<TableMetaTranslator> tableMetaTranslators = Lists.newArrayList(); // XXX
  @Getter
  @Setter
  private RecordPositioner positioner;
  @Getter
  @Setter
  private AlarmService alarmService;
  @Getter
  @Setter
  private String alarmReceiver;
  @Getter
  @Setter
  private TableController tableController;
  @Getter
  @Setter
  private ProgressTracer progressTracer;
  @Getter
  @Setter
  private StatAggregation statAggregation;
  @Getter
  @Setter
  private DbType targetDbType;
  @Getter
  @Setter
  private Thread worker = null;
  @Getter
  @Setter
  private volatile boolean extractorDump = true;
  @Getter
  @Setter
  private volatile boolean applierDump = true;
  @Getter
  @Setter
  private CountDownLatch mutex = new CountDownLatch(1);
  @Getter
  @Setter
  private YuGongException exception = null;
  @Getter
  @Setter
  private String tableShitKey;
  @Getter
  @Setter
  private int retryTimes = 1;
  @Getter
  @Setter
  private int retryInterval;
  @Getter
  @Setter
  private int noUpdateThresold;
  @Getter
  @Setter
  private int noUpdateTimes = 0;
  @Getter
  @Setter
  private boolean concurrent = true;
  @Getter
  @Setter
  private int threadSize = 5;
  @Getter
  @Setter
  private ThreadPoolExecutor executor;
  @Getter
  @Setter
  private String executorName;

  public YuGongInstance(YuGongContext context) {
    this.context = context;
    this.tableShitKey = context.getTableMeta().getFullName();
  }

  public void start() {
    MDC.put(YuGongConstants.MDC_TABLE_SHIT_KEY, tableShitKey);
    super.start();

    try {
      tableController.acquire();// ????????????

      executorName = this.getClass().getSimpleName() + "-" + context.getTableMeta().getFullName();
      if (executor == null) {
        executor = new ThreadPoolExecutor(threadSize,
            threadSize,
            60,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue(threadSize * 2),
            new NamedThreadFactory(executorName),
            new ThreadPoolExecutor.CallerRunsPolicy());
      }

      // ???????????????????????????????????????
      defaultTranslators.add(new OracleIncreamentDataTranslator());
      if (targetDbType.isOracle()) {
        defaultTranslators.add(new EncodeDataTranslator(context.getSourceEncoding(),
            context.getTargetEncoding())); // oracle?????????????????????????????????'UTF-8'???
      }

      if (!positioner.isStart()) {
        positioner.start();
      }

      Position lastPosition = positioner.getLatest();
      context.setLastPosition(lastPosition);

      if (!extractor.isStart()) {
        extractor.start();
      }

      if (!applier.isStart()) {
        applier.start();
      }

      worker = new Thread(new Runnable() {
        public void run() {
          try {
            if (context.getRunMode() != RunMode.INC) {
              // ???????????????inc???????????????
              retryTimes = 1;
            }

            for (int i = 0; i < retryTimes; i++) {
              MDC.remove(YuGongConstants.MDC_TABLE_SHIT_KEY);
              if (i > 0) {
                logger.info("table[{}] is start , retrying ", context.getTableMeta().getFullName());
              } else {
                logger.info("table[{}] is start", context.getTableMeta().getFullName());
              }

              try {
                // ??????????????????????????????????????????????????????????????????
                processTable();
                exception = null;
                break; // ?????????????????????
              } catch (YuGongException e) {
                exception = e;
                if (processException(e, i)) {
                  break;
                }
              } finally {
                MDC.remove(YuGongConstants.MDC_TABLE_SHIT_KEY);
              }
            }

            if (exception == null) {
              // ?????????????????????
              logger.info("table[{}] is end", context.getTableMeta().getFullName());
            } else if (ExceptionUtils.getRootCause(exception) instanceof InterruptedException) {
              progressTracer.update(context.getTableMeta().getFullName(), ProgressStatus.FAILED);
              logger.info("table[{}] is interrpt ,current status:{} !", context.getTableMeta()
                  .getFullName(), extractor.status());
            } else {
              progressTracer.update(context.getTableMeta().getFullName(), ProgressStatus.FAILED);
              logger.info("table[{}] is error , current status:{} !", context.getTableMeta()
                  .getFullName(), extractor.status());
            }
          } finally {
            tableController.release(YuGongInstance.this);
            // ???????????????
            mutex.countDown();
          }

        }

        private void processTable() {
          try {
            MDC.put(YuGongConstants.MDC_TABLE_SHIT_KEY, tableShitKey);
            ExtractStatus status = ExtractStatus.NORMAL;
            AtomicLong batchId = new AtomicLong(0);
            Position lastPosition = positioner.getLatest();
            context.setLastPosition(lastPosition);
            long tpsLimit = context.getTpsLimit();
            do {
              long start = System.currentTimeMillis();
              long extractorStartTime = System.currentTimeMillis();
              // ????????????
              List<Record> records = extractor.extract();
              logger.info("extract time {}", System.currentTimeMillis() - extractorStartTime);

              List<Record> ackRecords = records;// ??????ack??????
              if (YuGongUtils.isEmpty(records)) {
                status = extractor.status();
              }

              // ????????????????????????
              RecordDumper.dumpExtractorInfo(batchId.incrementAndGet(),
                  ackRecords,
                  lastPosition,
                  extractorDump);

              // ??????????????????translator??????
              for (DataTranslator translator : defaultTranslators) {
                records = processTranslator(translator, records);
              }

              // ????????????
              for (DataTranslator translator : translators) {
                records = processTranslator(translator, records);
              }

              // ????????????
              Throwable applierException = null;
              for (int i = 0; i < retryTimes; i++) {
                try {
                  long applyStartTime = System.currentTimeMillis();
                  applier.apply(records);
                  logger.info("apply time {}", System.currentTimeMillis() - applyStartTime);
                  applierException = null;
                  break;
                } catch (Throwable e) {
                  applierException = e;
                  if (processException(e, i)) {
                    break;
                  }
                }
              }

              if (applierException != null) {
                throw applierException;
              }

              // ??????ack?????????????????????
              Position position = extractor.ack(ackRecords);
              if (position != null) {
                // ?????????????????????????????????????????????????????????????????????
                positioner.persist(position);
              }

              context.setLastPosition(position);
              lastPosition = position;

              // ????????????????????????
              RecordDumper.dumpApplierInfo(batchId.get(), ackRecords, records, position, applierDump);

              long end = System.currentTimeMillis();

              if (tpsLimit > 0) {
                tpsControl(ackRecords, start, end, tpsLimit);
                end = System.currentTimeMillis();
              }

              if (YuGongUtils.isNotEmpty(ackRecords)) {
                statAggregation.push(new AggregationItem(start, end, Long.valueOf(ackRecords.size())));
              }

              // ???????????????????????????
              if (status == ExtractStatus.NO_UPDATE) {
                noUpdateTimes++;
                if (noUpdateThresold > 0 && noUpdateTimes > noUpdateThresold) {
                  break;
                }
              }
            } while (status != ExtractStatus.TABLE_END);

            logger.info("table[{}] is end by {}", context.getTableMeta().getFullName(), status);
            statAggregation.print();
          } catch (InterruptedException e) {
            // ??????????????????????????????
            throw new YuGongException(e);
          } catch (Throwable e) {
            throw new YuGongException(e);
          }
        }

        private List<Record> processTranslator(final DataTranslator translator, List<Record> records) {
          if (records.isEmpty()) {
            return records;
          }
          if (translator == null) {
            return records;
          }
          if (translator instanceof BackTableDataTranslator) {
            ExecutorTemplate template = null;
            if (concurrent) {
              template = new ExecutorTemplate(executor);
            }
            records = ((BackTableDataTranslator) translator).translator(context.getSourceDs(),
                context.getTargetDs(), records, template);
          } else {
            records = translator.translator(records);
          }

          return records;
        }

        private boolean processException(Throwable e, int i) {
          if (!(ExceptionUtils.getRootCause(e) instanceof InterruptedException)) {
            logger.error("retry {} ,something error happened. caused by {}",
                (i + 1),
                ExceptionUtils.getFullStackTrace(e));
            try {
              alarmService.sendAlarm(new AlarmMessage(ExceptionUtils.getFullStackTrace(e), alarmReceiver));
            } catch (Throwable e1) {
              logger.error("send alarm failed. ", e1);
            }

            try {
              Thread.sleep(retryInterval);
            } catch (InterruptedException e1) {
              exception = new YuGongException(e1);
              Thread.currentThread().interrupt();
              return true;
            }
          } else {
            // interrupt?????????????????????
            return true;
          }

          return false;
        }

      });

      worker.setUncaughtExceptionHandler(new YuGongUncaughtExceptionHandler(logger));
      worker.setName(this.getClass().getSimpleName() + "-" + context.getTableMeta().getFullName());
      worker.start();

      logger.info("table[{}] start successful. extractor:{} , applier:{}, translator:{}", new Object[]{
          context.getTableMeta().getFullName(), extractor.getClass().getName(), applier.getClass().getName(),
          translators != null ? translators: "NULL"});
    } catch (InterruptedException e) {
      progressTracer.update(context.getTableMeta().getFullName(), ProgressStatus.FAILED);
      exception = new YuGongException(e);
      mutex.countDown();
      tableController.release(this); // ?????????
      Thread.currentThread().interrupt();
    } catch (Throwable e) {
      progressTracer.update(context.getTableMeta().getFullName(), ProgressStatus.FAILED);
      exception = new YuGongException(e);
      mutex.countDown();
      logger.error("table[{}] start failed caused by {}",
          context.getTableMeta().getFullName(),
          ExceptionUtils.getFullStackTrace(e));
      tableController.release(this); // ?????????
    }
  }

  /**
   * ??????instance????????????
   */
  public void waitForDone() throws InterruptedException, YuGongException {
    mutex.await();

    if (exception != null) {
      throw exception;
    }
  }

  public void stop() {
    MDC.put(YuGongConstants.MDC_TABLE_SHIT_KEY, tableShitKey);
    super.stop();

    // ????????????
    if (worker != null) {
      worker.interrupt();
      try {
        worker.join(2 * 1000);
      } catch (InterruptedException e) {
        // ignore
      }
    }

    if (extractor.isStart()) {
      extractor.stop();
    }

    if (applier.isStart()) {
      applier.stop();
    }

    if (positioner.isStart()) {
      positioner.stop();
    }

    executor.shutdownNow();

    exception = null;
    logger.info("table[{}] stop successful. ", context.getTableMeta().getFullName());
  }

  private void tpsControl(List<Record> result, long start, long end, long tps) throws InterruptedException {
    long expectTime = (result.size() * 1000) / tps;
    long runTime = expectTime - (end - start);
    if (runTime > 0) {
      Thread.sleep(runTime);
    }
  }

}
