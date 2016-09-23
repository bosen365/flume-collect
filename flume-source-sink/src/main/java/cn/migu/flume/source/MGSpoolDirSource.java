package cn.migu.flume.source;

import cn.migu.flume.configuration.MGSpoolDirConfig;
import cn.migu.flume.helper.MGReliableSpoolFileEventReader;
import com.cronutils.descriptor.CronDescriptor;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import it.sauronsoftware.cron4j.Scheduler;
import it.sauronsoftware.cron4j.SchedulingPattern;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.serialization.LineDeserializer;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static cn.migu.flume.configuration.MGSpoolDirConfig.*;
import static com.cronutils.model.CronType.UNIX;

/**
 * All rights Reserved, Designed by Migu.cn
 *
 * @Description: 发自定义采集 source，支持
 * - 监听目录
 * - 定时采集
 * - 配置文件名过滤规则
 * - 断点续传
 * - 文件传输完毕消息通知
 * @Author : ChenYao
 * @Version : v1.0
 * @Date : 2016/8/1 11:25
 */
public class MGSpoolDirSource extends AbstractSource implements
        Configurable, EventDrivenSource {

    private static final Logger logger = LoggerFactory.getLogger(MGSpoolDirSource.class);

    /* Config options */
    // 文件结束后缀
    private String completedSuffix;

    // 监听目录
    private String spoolDirectory;

    // 是否在header中带上文件绝对路径
    private boolean fileHeader;

    // header 中文件绝对路径对应的key
    private String fileHeaderKey;

    // 是否在header中带上 文件名
    private boolean basenameHeader;

    // header 中文件名对应的key
    private String basenameHeaderKey;

    // 批量读取大小
    private int batchSize;

    // 需忽略的文件名规则
    private String ignorePattern;

    // track 目录 选择有读写权限的目录
    private String trackerDirPath;

    // 序列化方式 默认line 按行
    private String deserializerType;

    // 序列化context
    private Context deserializerContext;

    // 编码格式，默认为 utf-8
    private String inputCharset;

    // decode 错误时的处理策略
    private DecodeErrorPolicy decodeErrorPolicy;

    // jmx 监听相关
    private SourceCounter sourceCounter;
    private MGReliableSpoolFileEventReader reader;
    private ScheduledExecutorService executor;
    private boolean backoff = true;
    private boolean hitChannelException = false;
    private int maxBackoff;
    private MGSpoolDirConfig.ConsumeOrder consumeOrder;

    // 传输完成头文件
    private String fileDoneHeaderTag;
    // cron 表达式
    private String cronExp;
    // 初始时延
    private long initialDelay;
    // 每次读取时间间隔
    private int pollNewFileDelay;
    // 匹配文件正则
    private String matchPattern;

    private Scheduler cronTask;
    private String deletePolicy;


    @Override
    public synchronized void start() {
        logger.info("SpoolDirectorySource source starting with directory: {}",
                spoolDirectory);


        File directory = new File(spoolDirectory);

        // 初始化 reader
        try {
            reader = new MGReliableSpoolFileEventReader.Builder()
                    .spoolDirectory(directory)
                    .completedSuffix(completedSuffix)
                    .ignorePattern(ignorePattern)
                    .matchPattern(matchPattern)
                    .trackerDirPath(trackerDirPath)
                    .annotateFileName(fileHeader)
                    .fileNameHeader(fileHeaderKey)
                    .annotateBaseName(basenameHeader)
                    .baseNameHeader(basenameHeaderKey)
                    .deserializerType(deserializerType)
                    .fileDoneHeader(fileDoneHeaderTag)
                    .deserializerContext(deserializerContext)
                    .inputCharset(inputCharset)
                    .decodeErrorPolicy(decodeErrorPolicy)
                    .consumeOrder(consumeOrder)
                    .deletePolicy(deletePolicy)
                    .build();
        } catch (IOException ioe) {
            throw new FlumeException("Error instantiating spooling event parser", ioe);
        }

        Runnable runner = new SpoolDirectoryRunnable(reader, sourceCounter);

        // 当同时都正确配置了，cron 优先级更高
        if (SchedulingPattern.validate(cronExp)) {
            // print cron info
            // CronUtils commons-lang3 3.4
            CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(UNIX);
            CronParser parser = new CronParser(cronDefinition);
            String desc = CronDescriptor.instance(Locale.CHINA).describe(parser.parse(cronExp));

            logger.info("get cron config: [ {} ] , will run {}", cronExp, desc);

            // use cron4j run
            cronTask = new Scheduler();
            cronTask.schedule(cronExp, runner);
            cronTask.start();
        } else {
            logger.warn("cron expression: {} is invalid or not specify", cronExp);
            logger.warn("first run after {} minutes, run every {} minutes", initialDelay, pollNewFileDelay);
            // run right now
            executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleWithFixedDelay(runner, initialDelay, pollNewFileDelay, TimeUnit.MINUTES);
        }

        super.start();
        logger.debug("MGSpoolDirectorySource source started");
        sourceCounter.start();
    }

    @Override
    public synchronized void stop() {
        if (null != executor) {
            executor.shutdown();
            try {
                executor.awaitTermination(10L, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                logger.info("Interrupted while awaiting termination", ex);
            }
            executor.shutdownNow();
        } else {
            cronTask.stop();
        }

        super.stop();
        sourceCounter.stop();
        logger.info("SpoolDir source {} stopped. Metrics: {}", getName(), sourceCounter);
    }

    @Override
    public String toString() {
        return "Spool Directory source " + getName() +
                ": { spoolDir: " + spoolDirectory + " }";
    }

    @Override
    public synchronized void configure(Context context) {
        spoolDirectory = context.getString(SPOOL_DIRECTORY);
        Preconditions.checkState(spoolDirectory != null,
                "Configuration must specify a spooling directory");

        completedSuffix = context.getString(SPOOLED_FILE_SUFFIX,
                DEFAULT_SPOOLED_FILE_SUFFIX);
        fileHeader = context.getBoolean(FILENAME_HEADER,
                DEFAULT_FILE_HEADER);
        fileHeaderKey = context.getString(FILENAME_HEADER_KEY,
                DEFAULT_FILENAME_HEADER_KEY);
        deletePolicy = context.getString(DELETE_POLICY, DEFAULT_DELETE_POLICY);
        basenameHeader = context.getBoolean(BASENAME_HEADER,
                DEFAULT_BASENAME_HEADER);
        basenameHeaderKey = context.getString(BASENAME_HEADER_KEY,
                DEFAULT_BASENAME_HEADER_KEY);
        batchSize = context.getInteger(BATCH_SIZE,
                DEFAULT_BATCH_SIZE);
        inputCharset = context.getString(INPUT_CHARSET, DEFAULT_INPUT_CHARSET);
        decodeErrorPolicy = DecodeErrorPolicy.valueOf(
                context.getString(DECODE_ERROR_POLICY, DEFAULT_DECODE_ERROR_POLICY)
                        .toUpperCase(Locale.ENGLISH));

        ignorePattern = context.getString(IGNORE_PAT, DEFAULT_IGNORE_PAT);
        trackerDirPath = context.getString(TRACKER_DIR, DEFAULT_TRACKER_DIR);

        deserializerType = context.getString(DESERIALIZER, DEFAULT_DESERIALIZER);
        deserializerContext = new Context(context.getSubProperties(DESERIALIZER +
                "."));

        consumeOrder = ConsumeOrder.valueOf(context.getString(CONSUME_ORDER,
                DEFAULT_CONSUME_ORDER.toString()).toUpperCase(Locale.ENGLISH));

        fileDoneHeaderTag = context.getString(FILE_DONE_HEADER_KEY, FILE_DONE_DEFAULT_DEFAULT);
        cronExp = context.getString(CRON_EXP, CRON_EXP_DEFAULT);

        initialDelay = context.getLong(INITIAL_DELAY, INITIAL_DELAY_DEFAULT);
        pollNewFileDelay = context.getInteger(POLL_NEW_DELAY, POLL_NEW_DELAY_DEFAULT);
        matchPattern = context.getString(MATCH_PAT, DEFAULT_MATCH_PAT);

        // "Hack" to support backwards compatibility with previous generation of
        // spooling directory source, which did not support deserializers
        Integer bufferMaxLineLength = context.getInteger(BUFFER_MAX_LINE_LENGTH);
        if (bufferMaxLineLength != null && deserializerType != null &&
                deserializerType.equalsIgnoreCase(DEFAULT_DESERIALIZER)) {
            deserializerContext.put(LineDeserializer.MAXLINE_KEY,
                    bufferMaxLineLength.toString());
        }

        maxBackoff = context.getInteger(MAX_BACKOFF, DEFAULT_MAX_BACKOFF);
        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }

        logger.info("---------------------------config------------------------");
        logger.info("dir: {}", this.spoolDirectory);
        logger.info("read batch size: {}", this.batchSize);
        logger.info("match regex: {}", this.matchPattern);
        logger.info("ignore regex: {}", this.ignorePattern);
        logger.info("cron expression: {}", this.cronExp);
        logger.info("delete policy: {}", this.deletePolicy);
        logger.info("[header]: file send complete tag: {}", this.fileDoneHeaderTag);
        logger.info("[header]: fileName: {}", this.basenameHeaderKey);
        logger.info("---------------------------config------------------------");

    }


    /**
     * The class always backs off, this exists only so that we can test without
     * taking a really long time.
     *
     * @param backoff - whether the source should backoff if the channel is full
     */
    @VisibleForTesting
    protected void setBackOff(boolean backoff) {
        this.backoff = backoff;
    }

    @VisibleForTesting
    protected boolean hitChannelException() {
        return hitChannelException;
    }

    @VisibleForTesting
    protected SourceCounter getSourceCounter() {
        return sourceCounter;
    }

    private class SpoolDirectoryRunnable implements Runnable {
        private MGReliableSpoolFileEventReader reader;
        private SourceCounter sourceCounter;

        public SpoolDirectoryRunnable(MGReliableSpoolFileEventReader reader,
                                      SourceCounter sourceCounter) {
            this.reader = reader;
            this.sourceCounter = sourceCounter;
        }

        @Override
        public void run() {
            int backoffInterval = 250;
            try {
                while (!Thread.interrupted()) {

                    // 每次批量读取event
                    List<Event> events = reader.readEvents(batchSize);

                    if (events.isEmpty()) {
                        logger.info("events is empty, break current thread");
                        break;
                    }
                    sourceCounter.addToEventReceivedCount(events.size());
                    sourceCounter.incrementAppendBatchReceivedCount();

                    try {
                        // 取出的 event 交付 channel
                        getChannelProcessor().processEventBatch(events);
                        reader.commit();
                    } catch (ChannelException ex) {
                        // channel 满了 休眠等待
                        logger.warn("The channel is full, and cannot write data now. The " +
                                "source will try again after " + String.valueOf(backoffInterval) +
                                " milliseconds");
                        hitChannelException = true;
                        if (backoff) {
                            TimeUnit.MILLISECONDS.sleep(backoffInterval);
                            backoffInterval = backoffInterval << 1;
                            backoffInterval = backoffInterval >= maxBackoff ? maxBackoff :
                                    backoffInterval;
                        }
                        continue;
                    }
                    backoffInterval = 250;
                    sourceCounter.addToEventAcceptedCount(events.size());
                    sourceCounter.incrementAppendBatchAcceptedCount();
                }
            } catch (Throwable t) {
                logger.error("FATAL: " + MGSpoolDirSource.this.toString() + ": " +
                        "Uncaught exception in SpoolDirectorySource thread. " +
                        "Restart or reconfigure Flume to continue processing.", t);
                Throwables.propagate(t);
            }
        }
    }
}
