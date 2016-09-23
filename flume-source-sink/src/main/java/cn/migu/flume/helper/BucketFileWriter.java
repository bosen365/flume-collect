package cn.migu.flume.helper;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * All rights Reserved, Designed by Migu.cn
 *
 * @Description: file sink writer
 * @Author : ChenYao
 * @Version : v1.0
 * @Date : 2016/8/1 11:25
 * @Update : v1.1
 * 1. 对于文件传输过程中发生更新的文件 重命名后缀为 .uncompleted
 * 2.文件写数据改为 append 模式
 */
public class BucketFileWriter {

    private static final Logger logger = LoggerFactory.getLogger(BucketFileWriter.class);
    // 临时文件后缀
    private static final String IN_USE_EXT = ".tmp";
    // 重传文件后缀
    private static final String REDO_FILE_SUFFIX = ".redo";
    // 文件传输过程中 文件发生更新/修改的 文件后缀
    private static final String FILE_NOT_COMPLETED_SUFFIX = ".uncompleted";

    /**
     * This lock ensures that only one thread can open a file at a time.
     */
    private final AtomicLong fileExtensionCounter;
    private OutputStream outputStream;
    private EventSerializer serializer;

    // 文件最汇总持久化路径
    private String filePath;
    // 临时文件路径
    private String dstTmpFile;

    /**
     * Close the file handle and rename the temp file to the permanent filename.
     * Safe to call multiple times. Logs HDFSWriter.close() exceptions.
     *
     * @throws IOException On failure to rename if temp file exists.
     */

    public BucketFileWriter() {
        fileExtensionCounter = new AtomicLong(System.currentTimeMillis());
    }

    public String getCounterTime() {
        return new DateTime(fileExtensionCounter.get())
                .toString("yyyy-MM-dd_HH:mm:ss");
    }

    public void open(final String filePath, String serializerType,
                     Context serializerContext, final long rollInterval,
                     final ScheduledExecutorService timedRollerPool,
                     final FileWriterLinkedHashMap sfWriters) throws IOException {

        this.dstTmpFile = getDstTmpFile(filePath);
        File file = new File(this.dstTmpFile);
        file.getParentFile().mkdirs();

        // file write mode : append
        outputStream = new BufferedOutputStream(new FileOutputStream(file, true));
        logger.info("open file, filename = " + file.getAbsolutePath());
        serializer = EventSerializerFactory.getInstance(serializerType, serializerContext, outputStream);
        serializer.afterCreate();

        if (rollInterval > 0) {
            logger.info("batch flush for {} seconds ...", rollInterval);
            Callable<Void> action = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    logger.info(
                            "Rolling file ({}): Roll scheduled after {} sec elapsed.",
                            filePath + "_" + getCounterTime() + IN_USE_EXT,
                            rollInterval);
                    if (sfWriters.containsKey(filePath)) {
                        sfWriters.remove(filePath);
                    }

                    close();
                    return null;
                }
            };
            timedRollerPool.schedule(action, rollInterval, TimeUnit.SECONDS);
        }
    }


    /**
     * 获取临时写文件 文件名
     *
     * @param filePath dst 文件路径
     * @return
     */
    private String getDstTmpFile(String filePath) {

        // if file exist, add redo suffix
        if (new File(filePath).exists()) {
            return getDstTmpFile(filePath + REDO_FILE_SUFFIX);
        }

        this.filePath = filePath;
        return filePath + IN_USE_EXT;
    }

    public void append(Event event) throws IOException {
        serializer.write(event);
    }


    public void flush() throws IOException {
        // flush event to outputStream
        serializer.flush();
        // flush stream to disk
        outputStream.flush();
    }


    /**
     * Rename bucketPath file from .tmp to permanent location if success
     * Rename bucketPath file from .tmp to .uncompleted suffix if failed
     *
     * @param success 文件是否完整 传输成功
     */
    public void renameBucket(boolean success) {
        File srcPath = new File(dstTmpFile);
        String dstPath = success ? filePath : filePath + FILE_NOT_COMPLETED_SUFFIX;

        File dstFile = new File(dstPath);
        if (srcPath.exists()) {
            String msg = "Renaming " + srcPath + " to " + dstPath;
            if (srcPath.renameTo(dstFile)) {
                logger.info(msg);
            } else {
                logger.error(msg + " failed");
            }
        }
    }

    /**
     * flush 文件并关闭
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public synchronized void close() throws IOException, InterruptedException {
        if (outputStream != null) {
            outputStream.flush();
            outputStream.close();
        }
    }
}
