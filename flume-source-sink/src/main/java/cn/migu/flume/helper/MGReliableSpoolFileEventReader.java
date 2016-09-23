package cn.migu.flume.helper;


import cn.migu.flume.configuration.MGSpoolDirConfig;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.client.avro.ReliableEventReader;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.*;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Pattern;

import static cn.migu.flume.configuration.MGSpoolDirConfig.DATE_FORMAT;


/**
 * All rights Reserved, Designed by Migu.cn
 *
 * @Description: source reader 类
 * @Author : ChenYao
 * @Version : v1.0
 * @Date : 2016/6/24 11:25
 */
public class MGReliableSpoolFileEventReader implements ReliableEventReader {

    private static final Logger logger = LoggerFactory.getLogger(MGReliableSpoolFileEventReader.class);

    private static final String metaFileName = ".flumespool-main.meta";

    private final File spoolDirectory;
    private final String completedSuffix;
    private final String deserializerType;
    private final Context deserializerContext;
    private final Pattern ignorePattern;
    private final Pattern matchPattern;
    private final File metaFile;
    private final boolean annotateFileName;
    private final boolean annotateBaseName;
    private final String fileNameHeader;
    private final String deletePolicy;
    private final String baseNameHeader;
    private final Charset inputCharset;
    private String fileDoneHeader;
    private final DecodeErrorPolicy decodeErrorPolicy;
    private final MGSpoolDirConfig.ConsumeOrder consumeOrder;
    private final File doneFiles;

    private static final String doneFilesName = ".done-files.meta";
    private Optional<FileInfo> currentFile = Optional.absent();
    private List<String> doneFileList;

    /**
     * Always contains the last file from which lines have been read.
     */
    private Optional<FileInfo> lastFileRead = Optional.absent();
    private boolean committed = true;

    /**
     * Instance var to Cache directory listing
     */
    private Iterator<File> candidateFileIter = null;

    /**
     * Create a ReliableSpoolingFileEventReader to watch the given directory.
     */
    private MGReliableSpoolFileEventReader(File spoolDirectory,
                                           String completedSuffix, String ignorePattern, String matchPattern, String trackerDirPath,
                                           boolean annotateFileName, String fileNameHeader,
                                           boolean annotateBaseName, String baseNameHeader,
                                           String deserializerType, Context deserializerContext,
                                           String fileDoneTag, String inputCharset,
                                           DecodeErrorPolicy decodeErrorPolicy,
                                           MGSpoolDirConfig.ConsumeOrder consumeOrder, String deletePolicy) throws IOException {

        // Sanity checks
        Preconditions.checkNotNull(spoolDirectory);
        Preconditions.checkNotNull(completedSuffix);
        Preconditions.checkNotNull(ignorePattern);
        Preconditions.checkNotNull(trackerDirPath);
        Preconditions.checkNotNull(deserializerType);
        Preconditions.checkNotNull(deserializerContext);
        Preconditions.checkNotNull(fileDoneTag);
        Preconditions.checkNotNull(inputCharset);
        Preconditions.checkNotNull(deletePolicy);


        if (logger.isDebugEnabled()) {
            logger.debug("Initializing {} with directory={}, metaDir={}, " +
                            "deserializer={}",
                    MGReliableSpoolFileEventReader.class.getSimpleName(),
                    spoolDirectory, trackerDirPath, deserializerType);
        }

        // Verify directory exists and is readable/writable
        Preconditions.checkState(spoolDirectory.exists(),
                "Directory does not exist: " + spoolDirectory.getAbsolutePath());
        Preconditions.checkState(spoolDirectory.isDirectory(),
                "Path is not a directory: " + spoolDirectory.getAbsolutePath());

        this.spoolDirectory = spoolDirectory;
        this.completedSuffix = completedSuffix;
        this.deserializerType = deserializerType;
        this.deserializerContext = deserializerContext;
        this.annotateFileName = annotateFileName;
        this.fileNameHeader = fileNameHeader;
        this.annotateBaseName = annotateBaseName;
        this.deletePolicy = deletePolicy;
        this.baseNameHeader = baseNameHeader;
        this.ignorePattern = Pattern.compile(ignorePattern);
        this.matchPattern = Pattern.compile(matchPattern);
        this.inputCharset = Charset.forName(inputCharset);
        this.decodeErrorPolicy = Preconditions.checkNotNull(decodeErrorPolicy);
        this.consumeOrder = Preconditions.checkNotNull(consumeOrder);
        this.fileDoneHeader = fileDoneTag;

        File trackerDirectory = new File(trackerDirPath);

        // if relative path, treat as relative to spool directory
        if (!trackerDirectory.isAbsolute()) {
            trackerDirectory = new File(spoolDirectory, trackerDirPath);
        }

        // ensure that meta directory exists
        if (!trackerDirectory.exists()) {
            if (!trackerDirectory.mkdir()) {
                throw new IOException("Unable to mkdir nonexistent meta directory " +
                        trackerDirectory);
            }
        }

        // ensure that the meta directory is a directory
        if (!trackerDirectory.isDirectory()) {
            throw new IOException("Specified meta directory is not a directory" +
                    trackerDirectory);
        }

        this.metaFile = new File(trackerDirectory, metaFileName);
        if (metaFile.exists() && metaFile.length() == 0) {
            deleteMetaFile();
        }

        this.doneFiles = new File(trackerDirectory, doneFilesName);
        initDoneFiles();
    }

    /**
     * 在启动时加载已读文件列表
     */
    private void initDoneFiles() {
        this.doneFileList = getDoneFiles();
    }

    /**
     * Return the filename which generated the data from the last successful
     * {@link #readEvents(int)} call. Returns null if called before any file
     * contents are read.
     */
    public String getLastFileRead() {
        if (!lastFileRead.isPresent()) {
            return null;
        }
        return lastFileRead.get().getFile().getAbsolutePath();
    }

    // public interface
    public Event readEvent() throws IOException {
        List<Event> events = readEvents(1);
        if (!events.isEmpty()) {
            return events.get(0);
        } else {
            return null;
        }
    }

    public List<Event> readEvents(int numEvents) throws IOException {
        if (!committed) {
            if (!currentFile.isPresent()) {
                throw new IllegalStateException("File should not roll when " +
                        "commit is outstanding.");
            }
            // 读取新数据前，上一次还没有 commit 则 reset 游标
            logger.info("Last read was never committed - resetting mark position.");
            currentFile.get().getDeserializer().reset();
        } else {
            // Check if new files have arrived since last call
            if (!currentFile.isPresent()) {
                logger.info("Check if new files have arrived since last call");
                currentFile = getNextFile();
            }
            // Return empty list if no new files
            if (!currentFile.isPresent()) {
                logger.info("Return empty list for no new files");
                return Collections.emptyList();
            }
        }

        EventDeserializer des = currentFile.get().getDeserializer();
        List<Event> events = des.readEvents(numEvents);

        // 当前文件已经读完, 记录已读文件，发送文件读取完毕消息
        if (events.isEmpty()) {
            String fileName = currentFile.get().getFile().getName();
            logger.info("file: {} read is done, will record filename and send msg", fileName);

            boolean fileChanged = retireCurrentFile();
            Map<String, String> tag = Maps.newHashMap();

            // 通知文件传完
            // 如果文件没发生更新，则 put true
            // 如果文件传输中发生更新，则 put false
            tag.put(this.fileDoneHeader, Boolean.toString(!fileChanged));
            // 文件的绝对路径
            tag.put(fileNameHeader, currentFile.get().getFile().getAbsolutePath());
            // 文件名
            tag.put(baseNameHeader, fileName);

            lastFileRead = currentFile;
            currentFile = Optional.absent();

            return Collections.singletonList(EventBuilder.withBody("".getBytes(), tag));
        }


        if (annotateFileName) {
            String filename = currentFile.get().getFile().getAbsolutePath();
            for (Event event : events) {
                event.getHeaders().put(fileNameHeader, filename);
            }
        }

        if (annotateBaseName) {
            String basename = currentFile.get().getFile().getName();
            for (Event event : events) {
                event.getHeaders().put(baseNameHeader, basename);
            }
        }

        committed = false;
        lastFileRead = currentFile;
        return events;
    }

    @Override
    public void close() throws IOException {
        if (currentFile.isPresent()) {
            currentFile.get().getDeserializer().close();
            currentFile = Optional.absent();
        }
    }

    /**
     * Commit the last lines which were read.
     */
    @Override
    public void commit() throws IOException {
        if (!committed && currentFile.isPresent()) {
            currentFile.get().getDeserializer().mark();
            committed = true;
        }
    }


    /**
     * Closes currentFile and attempt to rename it.
     * <p/>
     * If these operations fail in a way that may cause duplicate log entries,
     * an error is logged but no exceptions are thrown. If these operations fail
     * in a way that indicates potential misuse of the spooling directory, a
     * FlumeException will be thrown.
     *
     * @throws FlumeException if files do not conform to spooling assumptions
     */
    private boolean retireCurrentFile() throws IOException {
        Preconditions.checkState(currentFile.isPresent());

        File fileToRoll = new File(currentFile.get().getFile().getAbsolutePath());

        currentFile.get().getDeserializer().close();

        boolean fileChanged = false;
        // Verify that spooling assumptions hold

        if (fileToRoll.lastModified() != currentFile.get().getLastModified()) {
            String message = "!!!! File has been modified since being read: " + fileToRoll;
            fileChanged = true;
            logger.error(message);
            logger.error("read last modify time:{}, but current is {}", currentFile.get().getLastModified(), fileToRoll.lastModified());
        }

        if (fileToRoll.length() != currentFile.get().getLength()) {
            String message = "!!!! File has changed size since being read: " + fileToRoll;
            fileChanged = true;
            logger.error(message);
            logger.error("read size: {}, but current size: {}", currentFile.get().getLength(), fileToRoll.length());
        }

        // delete metaFile
        deleteMetaFile();

        if (!fileChanged) {
            // record done files
            logger.info("upload done, now record filename");
            recordDoneFiles();
            if (deletePolicy.equalsIgnoreCase(DeletePolicy.IMMEDIATE.name())) {
                deleteCurrentFile(fileToRoll);
            }
        } else {
            logger.warn("will not record file, and redo collect next time if file exist");
        }

        return fileChanged;
    }

    /**
     * 记录已采集文件
     * 1. add to doneFile list
     * 2. record to file
     * <p>
     * update: 更新日期格式，减少空间占用
     *
     * @throws IOException
     */
    private synchronized void recordDoneFiles() throws IOException {
        // save file uploaded
        if (!this.doneFiles.exists()) {
            logger.info("record done files do not exist , now create new");
            Files.touch(this.doneFiles);
        }

        // write a line with date and filename
        String fileName = currentFile.get().getFile().getName();
        doneFileList.add(fileName);

        String line = DateTime.now().toString(DATE_FORMAT) + "\t" + fileName;
        Files.append(line + "\n", this.doneFiles, Charsets.UTF_8);
    }

    /**
     * Delete the given spooled file
     *
     * @param fileToDelete
     * @throws IOException
     */
    private void deleteCurrentFile(File fileToDelete) throws IOException {
        logger.info("Preparing to delete file {}", fileToDelete);
        if (!fileToDelete.exists()) {
            logger.warn("Unable to delete nonexistent file: {}", fileToDelete);
            return;
        }
        if (!fileToDelete.delete()) {
            throw new IOException("Unable to delete spool file: " + fileToDelete);
        }
        // now we no longer need the meta file
        deleteMetaFile();
    }


    /**
     * get files that has already been uploaded
     * <p>
     * update v2.0 性能优化，不用每次都重新读，第一次读便缓存，每次读完 append 即可
     */
    private List<String> getDoneFiles() {
        List<String> doneFiles = Lists.newArrayList();
        if (!this.doneFiles.exists()) {
            logger.warn("done record file does not exist now");
            return doneFiles;
        }

        try {
            for (String s : Files.readLines(this.doneFiles, Charsets.UTF_8)) {
                if (!s.isEmpty() && s.contains("\t")) {
                    doneFiles.add(s.split("\t")[1].trim());
                }
            }
        } catch (IOException e) {
            logger.error("can't open done-record-file: {}", this.doneFiles.getAbsolutePath());
        }

        return doneFiles;
    }

    /**
     * Returns the next file to be consumed from the chosen directory.
     * If the directory is empty or the chosen file is not readable,
     * If two or more files are equally old/young, then the file name with
     * lower lexicographical value is returned.
     * <p>
     * 扫描目录,根据指定规则，获取目标文件
     * 过滤规则：
     * 1.排除目录 隐藏文件 已经读取传输完毕的文件
     * 2.按照消费策略有序读取（默认 old -> young）
     * 3.根据正则筛选所需(matchPattern)，过滤文件(ignorePattern)
     * <p>
     * <p>
     */
    private Optional<FileInfo> getNextFile() {
        List<File> candidateFiles = Collections.emptyList();

        if (consumeOrder != MGSpoolDirConfig.ConsumeOrder.RANDOM ||
                candidateFileIter == null ||
                !candidateFileIter.hasNext()) {

            /* Filter to exclude finished or hidden files */
            FileFilter filter = new FileFilter() {
                public boolean accept(File candidate) {
                    String fileName = candidate.getName();
                    if ((candidate.isDirectory()) ||
                            (fileName.startsWith(".")) ||
                            doneFileList.contains(fileName) ||
                            ignorePattern.matcher(fileName).matches() ||
                            !matchPattern.matcher(fileName).matches()
                            ) {
                        return false;
                    }
                    return true;
                }
            };
            candidateFiles = Arrays.asList(spoolDirectory.listFiles(filter));
            candidateFileIter = candidateFiles.iterator();
        }

        if (!candidateFileIter.hasNext()) { // No matching file in spooling directory.
            return Optional.absent();
        }

        File selectedFile = candidateFileIter.next();
        if (consumeOrder == MGSpoolDirConfig.ConsumeOrder.RANDOM) { // Selected file is random.
            return openFile(selectedFile);
        } else if (consumeOrder == MGSpoolDirConfig.ConsumeOrder.YOUNGEST) {
            for (File candidateFile : candidateFiles) {
                long compare = selectedFile.lastModified() -
                        candidateFile.lastModified();
                if (compare == 0) { // ts is same pick smallest lexicographically.
                    selectedFile = smallerLexicographical(selectedFile, candidateFile);
                } else if (compare < 0) { // candidate is younger (cand-ts > selec-ts)
                    selectedFile = candidateFile;
                }
            }
        } else { // default order is OLDEST
            for (File candidateFile : candidateFiles) {
                long compare = selectedFile.lastModified() -
                        candidateFile.lastModified();
                if (compare == 0) { // ts is same pick smallest lexicographically.
                    selectedFile = smallerLexicographical(selectedFile, candidateFile);
                } else if (compare > 0) { // candidate is older (cand-ts < selec-ts).
                    selectedFile = candidateFile;
                }
            }
        }

        return openFile(selectedFile);
    }

    private File smallerLexicographical(File f1, File f2) {
        if (f1.getName().compareTo(f2.getName()) < 0) {
            return f1;
        }
        return f2;
    }

    /**
     * Opens a file for consuming
     *
     * @param file file does not exists or readable.
     */
    private Optional<FileInfo> openFile(File file) {
        try {
            // roll the meta file, if needed
            logger.info("open a file for consuming: {}", file.getName());
            String nextPath = file.getPath();
            PositionTracker tracker =
                    DurablePositionTracker.getInstance(metaFile, nextPath);
            if (!tracker.getTarget().equals(nextPath)) {
                tracker.close();
                deleteMetaFile();
                tracker = DurablePositionTracker.getInstance(metaFile, nextPath);
            }

            // sanity check
            Preconditions.checkState(tracker.getTarget().equals(nextPath),
                    "Tracker target %s does not equal expected filename %s",
                    tracker.getTarget(), nextPath);

            ResettableInputStream in =
                    new ResettableFileInputStream(file, tracker,
                            ResettableFileInputStream.DEFAULT_BUF_SIZE, inputCharset,
                            decodeErrorPolicy);
            EventDeserializer deserializer = EventDeserializerFactory.getInstance
                    (deserializerType, deserializerContext, in);

            return Optional.of(new MGReliableSpoolFileEventReader.FileInfo(file, deserializer));
        } catch (FileNotFoundException e) {
            // File could have been deleted in the interim
            logger.warn("Could not find file: " + file, e);
            return Optional.absent();
        } catch (IOException e) {
            logger.error("Exception opening file: " + file, e);
            return Optional.absent();
        }
    }

    private void deleteMetaFile() throws IOException {
        if (metaFile.exists() && !metaFile.delete()) {
            throw new IOException("Unable to delete old meta file " + metaFile);
        }
    }

    /**
     * An immutable class with information about a file being processed.
     */
    private static class FileInfo {
        private final File file;
        private final long length;
        private final long lastModified;
        private final EventDeserializer deserializer;

        public FileInfo(File file, EventDeserializer deserializer) {
            this.file = file;
            this.length = file.length();
            this.lastModified = file.lastModified();
            this.deserializer = deserializer;
        }

        public long getLength() {
            return length;
        }

        public long getLastModified() {
            return lastModified;
        }

        public EventDeserializer getDeserializer() {
            return deserializer;
        }

        public File getFile() {
            return file;
        }
    }

    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    static enum DeletePolicy {
        NEVER,
        IMMEDIATE,
        DELAY
    }

    /**
     * Special builder class for ReliableSpoolingFileEventReader
     */
    public static class Builder {
        private File spoolDirectory;
        private String completedSuffix =
                MGSpoolDirConfig.SPOOLED_FILE_SUFFIX;
        private String ignorePattern =
                MGSpoolDirConfig.DEFAULT_IGNORE_PAT;
        private String trackerDirPath =
                MGSpoolDirConfig.DEFAULT_TRACKER_DIR;
        private Boolean annotateFileName =
                MGSpoolDirConfig.DEFAULT_FILE_HEADER;
        private String fileNameHeader =
                MGSpoolDirConfig.DEFAULT_FILENAME_HEADER_KEY;
        private Boolean annotateBaseName =
                MGSpoolDirConfig.DEFAULT_BASENAME_HEADER;
        private String baseNameHeader =
                MGSpoolDirConfig.DEFAULT_BASENAME_HEADER_KEY;
        private String deserializerType =
                MGSpoolDirConfig.DEFAULT_DESERIALIZER;
        private Context deserializerContext = new Context();
        private String fileDoneHeader =
                MGSpoolDirConfig.FILE_DONE_DEFAULT_DEFAULT;
        private String deletePolicy =
                MGSpoolDirConfig.DEFAULT_DELETE_POLICY;
        private String inputCharset =
                MGSpoolDirConfig.DEFAULT_INPUT_CHARSET;
        private DecodeErrorPolicy decodeErrorPolicy = DecodeErrorPolicy.valueOf(
                MGSpoolDirConfig.DEFAULT_DECODE_ERROR_POLICY
                        .toUpperCase(Locale.ENGLISH));
        private MGSpoolDirConfig.ConsumeOrder consumeOrder =
                MGSpoolDirConfig.DEFAULT_CONSUME_ORDER;
        private String matchPattern = MGSpoolDirConfig.DEFAULT_MATCH_PAT;

        public MGReliableSpoolFileEventReader.Builder spoolDirectory(File directory) {
            this.spoolDirectory = directory;
            return this;
        }

        public MGReliableSpoolFileEventReader.Builder completedSuffix(String completedSuffix) {
            this.completedSuffix = completedSuffix;
            return this;
        }

        public MGReliableSpoolFileEventReader.Builder ignorePattern(String ignorePattern) {
            this.ignorePattern = ignorePattern;
            return this;
        }

        public MGReliableSpoolFileEventReader.Builder matchPattern(String matchPattern) {
            this.matchPattern = matchPattern;
            return this;
        }


        public MGReliableSpoolFileEventReader.Builder trackerDirPath(String trackerDirPath) {
            this.trackerDirPath = trackerDirPath;
            return this;
        }

        public MGReliableSpoolFileEventReader.Builder annotateFileName(Boolean annotateFileName) {
            this.annotateFileName = annotateFileName;
            return this;
        }

        public MGReliableSpoolFileEventReader.Builder fileNameHeader(String fileNameHeader) {
            this.fileNameHeader = fileNameHeader;
            return this;
        }

        public MGReliableSpoolFileEventReader.Builder annotateBaseName(Boolean annotateBaseName) {
            this.annotateBaseName = annotateBaseName;
            return this;
        }

        public MGReliableSpoolFileEventReader.Builder baseNameHeader(String baseNameHeader) {
            this.baseNameHeader = baseNameHeader;
            return this;
        }

        public MGReliableSpoolFileEventReader.Builder deserializerType(String deserializerType) {
            this.deserializerType = deserializerType;
            return this;
        }

        public MGReliableSpoolFileEventReader.Builder deserializerContext(Context deserializerContext) {
            this.deserializerContext = deserializerContext;
            return this;
        }

        public MGReliableSpoolFileEventReader.Builder deletePolicy(String deletePolicy) {
            this.deletePolicy = deletePolicy;
            return this;
        }

        public MGReliableSpoolFileEventReader.Builder inputCharset(String inputCharset) {
            this.inputCharset = inputCharset;
            return this;
        }

        public MGReliableSpoolFileEventReader.Builder decodeErrorPolicy(DecodeErrorPolicy decodeErrorPolicy) {
            this.decodeErrorPolicy = decodeErrorPolicy;
            return this;
        }

        public MGReliableSpoolFileEventReader.Builder consumeOrder(MGSpoolDirConfig.ConsumeOrder consumeOrder) {
            this.consumeOrder = consumeOrder;
            return this;
        }

        public MGReliableSpoolFileEventReader.Builder fileDoneHeader(String fileDoneTag) {
            this.fileDoneHeader = fileDoneTag;
            return this;
        }

        public MGReliableSpoolFileEventReader build() throws IOException {
            return new MGReliableSpoolFileEventReader(spoolDirectory, completedSuffix,
                    ignorePattern, matchPattern, trackerDirPath, annotateFileName, fileNameHeader,
                    annotateBaseName, baseNameHeader, deserializerType,
                    deserializerContext, fileDoneHeader, inputCharset, decodeErrorPolicy,
                    consumeOrder, deletePolicy);
        }


    }

}

