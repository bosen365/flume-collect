package cn.migu.flume.helper;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

/**
 * All rights Reserved, Designed by Migu.cn
 *
 * @Description: file writer Hash 链表, 当 writer handler 超过最大值，移除最旧的那一个
 * @Author : ChenYao
 * @Version : v1.0
 * @Date : 2016/8/1 11:25
 */
public class FileWriterLinkedHashMap extends
        LinkedHashMap<String, BucketFileWriter> {

    private static final Logger logger = LoggerFactory
            .getLogger(FileWriterLinkedHashMap.class);

    private static final long serialVersionUID = -7860596835613215998L;
    private final int maxOpenFiles;

    public FileWriterLinkedHashMap(int maxOpenFiles) {
        super(16, 0.75f, true); // stock initial capacity/load, access
        this.maxOpenFiles = maxOpenFiles;
    }

    @Override
    protected boolean removeEldestEntry(Entry<String, BucketFileWriter> eldest) {
        if (size() > maxOpenFiles) {
            // If we have more that max open files, then close the last one
            // and
            // return true
            try {
                eldest.getValue().close();
            } catch (IOException e) {
                logger.warn(eldest.getKey(), e);
            } catch (InterruptedException e) {
                logger.warn(eldest.getKey(), e);
                Thread.currentThread().interrupt();
            }
            return true;
        } else {
            return false;
        }
    }
}

