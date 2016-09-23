package cn.migu.flume.sink;

import cn.migu.flume.helper.BucketFileWriter;
import cn.migu.flume.helper.FileWriterLinkedHashMap;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


/**
 * All rights Reserved, Designed by Migu.cn
 *
 * @Description: 自定义sink，支持
 * - 持久化文件到本地目录
 * - 按 主机名/文件名 归档
 * @Author : ChenYao
 * @Version : v1.0
 * @Date : 2016/2/23 11:25
 * @update : v1.1
 * 1.对于文件传输过程中发生更新的文件 重命名后缀为 .uncompleted
 * 2.文件写数据改为 append 模式
 */
public class MGSpoolFileSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(MGSpoolFileSink.class);

    // 持久化输出目录
    private String directory;
    // 序列化类型
    private String serializerType;
    private Context serializerContext;
    // writers
    private FileWriterLinkedHashMap sfWriters;
    // 调度器
    private ScheduledExecutorService timedRollerPool;
    // roll 时间间隔 默认为 0，即不按时间滚动文件
    private long rollInterval;
    // 最大打开文件句柄
    private int maxOpenFiles;

    // 主机名 header key 默认为 hostname
    private static final String HOST_HEADER_KEY = "HOST_HEADER_KEY";
    private static final String HOST_NAME_DEFAULT = "hostname";
    private String hostHeaderName;

    // 与source对应 , 文件传完的结束标签 默认为 fileDone
    private static final String DONE_FILES_TAG_KEY = "doneFilesTag";
    private static final String DONE_FILES_TAG_DEFAULT = "fileDone";
    private String doneFilesTag;

    // 最大批量读取event数 超过即 flush 默认 100
    private long txnEventMax;
    private static final long TXN_EVENT_MAX_DEFAULT = 100;
    private static final String BATCH_EVENT_KEY = "batchEvent";
    private SinkCounter sinkCounter;

    // 文件名对应的 header 默认为 basename
    private String fileNameHeader;
    private static final String FILE_NAME_HEADER_DEFAULT = "basename";


    public void configure(Context context) {
        String directory = context.getString("sink.directory");
        serializerType = context.getString("sink.serializer", "TEXT");
        serializerContext =
                new Context(context.getSubProperties("sink." +
                        EventSerializer.CTX_PREFIX));

        Preconditions.checkArgument(directory != null, "Directory may not be null");
        Preconditions.checkNotNull(serializerType, "Serializer type is undefined");

        maxOpenFiles = context.getInteger("file.maxOpenFiles", 30);
        rollInterval = context.getLong("file.rollInterval", 0L);

        String rollerName = "file-" + getName() + "-roll-timer-%d";
        timedRollerPool = Executors.newScheduledThreadPool(maxOpenFiles,
                new ThreadFactoryBuilder().setNameFormat(rollerName).build());
        this.directory = directory;
        this.hostHeaderName = context.getString(HOST_HEADER_KEY, HOST_NAME_DEFAULT);
        this.doneFilesTag = context.getString(DONE_FILES_TAG_KEY, DONE_FILES_TAG_DEFAULT);
        this.txnEventMax = context.getLong(BATCH_EVENT_KEY, TXN_EVENT_MAX_DEFAULT);
        this.fileNameHeader = context.getString("file.fileNameHeader", FILE_NAME_HEADER_DEFAULT);

        logger.info("------------------- config ---------------------");
        logger.info("output dir: {}", directory);
        logger.info("host header name: {}", hostHeaderName);
        logger.info("rollInterval: every {}s", rollInterval);
        logger.info("batchEvent: {}", txnEventMax);
        logger.info("------------------- config ---------------------");

        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
    }

    public Status process() throws EventDeliveryException {

        List<BucketFileWriter> writers = Lists.newArrayList();

        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        Event event = null;
        Map<String, String> header;

        try {
            int txnEventCount;
            for (txnEventCount = 0; txnEventCount < txnEventMax; txnEventCount++) {
                event = channel.take();
                if (event == null) {
                    break;
                }

                header = event.getHeaders();
                String host = header.get(hostHeaderName);
                String fileName = header.get(fileNameHeader);
                String realPath = Joiner.on(File.separator).join(directory, host, fileName);
                BucketFileWriter bucketFileWriter = sfWriters.get(realPath);

                if (bucketFileWriter == null) {
                    bucketFileWriter = new BucketFileWriter();
                    bucketFileWriter.open(realPath, serializerType,
                            serializerContext, rollInterval, timedRollerPool,
                            sfWriters);
                    sfWriters.put(realPath, bucketFileWriter);
                }

                if (!writers.contains(bucketFileWriter)) {
                    writers.add(bucketFileWriter);
                }

                // 上一个文件传输完毕 flush and rename and commit
                if (header.containsKey(this.doneFilesTag)) {
                    logger.info("last send is done, host: {}, file: {}", host, fileName);
                    boolean success = Boolean.parseBoolean(header.get(this.doneFilesTag));

                    try {
                        // flush all pending buckets before committing the transaction
                        for (BucketFileWriter bf : writers) {
                            bf.flush();
                        }

                        bucketFileWriter.close();
                        bucketFileWriter.renameBucket(success);
                        sfWriters.remove(realPath);
                        writers.remove(bucketFileWriter);
                        transaction.commit();
                        if (txnEventCount > 0) {
                            sinkCounter.addToEventDrainSuccessCount(txnEventCount);
                        }

                        return Status.READY;

                    } catch (InterruptedException e) {
                        transaction.rollback();
                        logger.warn("File IO error", e);
                        return Status.BACKOFF;
                    }
                } else {
                    bucketFileWriter.append(event);
                }
            }

            if (txnEventCount == 0) {
                sinkCounter.incrementBatchEmptyCount();
            } else if (txnEventCount == txnEventMax) {
                sinkCounter.incrementBatchCompleteCount();
            } else {
                sinkCounter.incrementBatchUnderflowCount();
            }

            // flush all pending buckets before committing the transaction
            for (BucketFileWriter bucketFileWriter : writers) {
                bucketFileWriter.flush();
            }

            transaction.commit();
            if (txnEventCount > 0) {
                sinkCounter.addToEventDrainSuccessCount(txnEventCount);
            }

            if (event == null) {
                return Status.BACKOFF;
            }
            return Status.READY;
        } catch (IOException e) {
            transaction.rollback();
            logger.warn("File IO error", e);
            return Status.BACKOFF;
        } catch (Throwable th) {
            transaction.rollback();
            logger.error("process failed", th);
            if (th instanceof Error) {
                throw (Error) th;
            } else {
                throw new EventDeliveryException(th);
            }
        } finally {
            transaction.close();
        }
    }

    public synchronized void start() {
        super.start();
        this.sfWriters = new FileWriterLinkedHashMap(maxOpenFiles);
        sinkCounter.start();
    }
}
