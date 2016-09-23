package cn.migu.flume.configuration;

import org.apache.flume.serialization.DecodeErrorPolicy;

/**
 * All rights Reserved, Designed by Migu.cn
 *
 * @Description: 采集 source 相关的配置
 * @Author : ChenYao
 * @Version : v1.0
 * @Date : 2016/8/1 11:25
 */
public class MGSpoolDirConfig {


    /**
     * Directory where files are deposited.
     */
    public static final String SPOOL_DIRECTORY = "mgSpoolDir";

    /**
     * cron expression that control to schedule source
     * note: the priority of cron is super than run delay if both has config
     */
    public static final String CRON_EXP = "cronExp";
    public static final String CRON_EXP_DEFAULT = "";

    /**
     * initial delay of the runner (unit: minutes, default 0)
     */
    public static final String INITIAL_DELAY = "initialDelay";
    public static final long INITIAL_DELAY_DEFAULT = 0L;

    /**
     * interval of the runner (unit: minutes, default: 24 hours = 60*24 minutes)
     */
    public static final String POLL_NEW_DELAY = "pollNewDelay";
    public static final int POLL_NEW_DELAY_DEFAULT = 1440;

    /**
     * tag of header when file been sent complete
     */
    public static final String FILE_DONE_HEADER_KEY = "fileDoneHeaderKey";
    public static final String FILE_DONE_DEFAULT_DEFAULT = "fileDone";

    /**
     * Suffix appended to files when they are finished being sent.
     */
    public static final String SPOOLED_FILE_SUFFIX = "fileSuffix";
    public static final String DEFAULT_SPOOLED_FILE_SUFFIX = ".COMPLETED";

    /**
     * Header in which to put absolute path filename.
     */
    public static final String FILENAME_HEADER_KEY = "fileHeaderKey";
    public static final String DEFAULT_FILENAME_HEADER_KEY = "file";

    /**
     * Whether to include absolute path filename in a header.
     */
    public static final String FILENAME_HEADER = "fileHeader";
    public static final boolean DEFAULT_FILE_HEADER = true;

    /**
     * Header in which to put the basename of file.
     */
    public static final String BASENAME_HEADER_KEY = "basenameHeaderKey";
    public static final String DEFAULT_BASENAME_HEADER_KEY = "basename";

    /**
     * Whether to include the basename of a file in a header.
     */
    public static final String BASENAME_HEADER = "basenameHeader";
    public static final boolean DEFAULT_BASENAME_HEADER = true;

    /**
     * What size to batch with before sending to ChannelProcessor.
     */
    public static final String BATCH_SIZE = "batchSize";
    public static final int DEFAULT_BATCH_SIZE = 100;


    /**
     * Maximum number of lines to buffer between commits.
     */
    @Deprecated
    public static final String BUFFER_MAX_LINES = "bufferMaxLines";
    @Deprecated
    public static final int DEFAULT_BUFFER_MAX_LINES = 100;

    /**
     * Maximum length of line (in characters) in buffer between commits.
     */
    @Deprecated
    public static final String BUFFER_MAX_LINE_LENGTH = "bufferMaxLineLength";
    @Deprecated
    public static final int DEFAULT_BUFFER_MAX_LINE_LENGTH = 5000;

    /**
     * Pattern of files to ignore
     */
    public static final String IGNORE_PAT = "ignorePattern";
    public static final String DEFAULT_IGNORE_PAT = "^$"; // no effect

    /**
     * Pattern of files to collect
     */
    public static final String MATCH_PAT = "matchPattern";
    public static final String DEFAULT_MATCH_PAT = "^.*$"; // match all

    /**
     * Directory to store metadata about files being processed
     */
    public static final String TRACKER_DIR = "trackerDir";
    public static final String DEFAULT_TRACKER_DIR = ".flumespool";

    /**
     * Deserializer to use to parse the file data into Flume Events
     */
    public static final String DESERIALIZER = "deserializer";
    public static final String DEFAULT_DESERIALIZER = "LINE";

    public static final String DELETE_POLICY = "deletePolicy";
    public static final String DEFAULT_DELETE_POLICY = "never";

    /**
     * Character set used when reading the input.
     */
    public static final String INPUT_CHARSET = "inputCharset";
    public static final String DEFAULT_INPUT_CHARSET = "UTF-8";

    /**
     * What to do when there is a character set decoding error.
     */
    public static final String DECODE_ERROR_POLICY = "decodeErrorPolicy";
    public static final String DEFAULT_DECODE_ERROR_POLICY =
            DecodeErrorPolicy.FAIL.name();

    public static final String MAX_BACKOFF = "maxBackoff";

    public static final Integer DEFAULT_MAX_BACKOFF = 4000;

    /**
     * Consume order.
     */
    public enum ConsumeOrder {
        OLDEST, YOUNGEST, RANDOM
    }

    public static final String CONSUME_ORDER = "consumeOrder";
    public static final ConsumeOrder DEFAULT_CONSUME_ORDER = ConsumeOrder.OLDEST;

    public static final String DATE_FORMAT = "YYYY-MM-dd HH:mm:ss";
}
