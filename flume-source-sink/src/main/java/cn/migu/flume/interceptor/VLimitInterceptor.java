package cn.migu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;



/**
 * All rights Reserved, Designed by Migu.cn
 *
 * @Description: flume event 拦截器 进行流量限速
 * @Author : ChenYao
 * @Version : v1.0
 * @Date : 2016/6/29 11:25
 */
public class VLimitInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(VLimitInterceptor.class);

    private static final long KB = 1024L;
    private long lastEventSentTick = System.nanoTime();
    private long pastSentLength = 0L;

    private long threshold;

    // 1s = 10[9] nano seconds
    private static final long timeCostPerCheck = 1000000000L;
    private long headerSize = 0L;

    private int num = 0;

    public VLimitInterceptor(long limitRate, long headerSize) {
        this.threshold = (limitRate * KB);
        this.headerSize = headerSize;
        logger.info("threshold size: {}KB", limitRate);
    }

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        this.num += 1;

        if (this.pastSentLength > this.threshold) {
            long nowTick = System.nanoTime();
            long multiple = this.pastSentLength / this.threshold;

            // 累计到 threshold size 的耗时
            long duration = nowTick - this.lastEventSentTick;

            // 上次传输完 threshold 还剩的时间 (上次传输速度很快)
            // 剩下的时间
            long restTime = multiple * timeCostPerCheck - duration;

            // 提前传完, 剩下的时间需要休眠暂停
            if (restTime > 0L) {
                try {
                    logger.info("Limit source send rate," +
                            " pastSentLength:{}, sleepTime:{} ms, num event:{}\n",
                            pastSentLength, restTime / 1000000, num);

                    Thread.sleep(restTime / 1000000L, (int) (restTime % 1000000L));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            this.num = 0;
            this.pastSentLength = 0L;
            this.lastEventSentTick = (nowTick + (restTime > 0L ? restTime : 0L));
        }

        this.pastSentLength += headerSize + event.getBody().length;
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }


    @Override
    public void close() {
    }


    public static class Builder implements Interceptor.Builder {

        private long limitRate;
        private long headerSize;

        @Override
        public Interceptor build() {
            return new VLimitInterceptor(this.limitRate, this.headerSize);
        }

        @Override
        public void configure(Context context) {
            this.limitRate = context.getLong(Constants.LIMIT_RATE, Constants.DEFAULT_RATE);
            this.headerSize = context.getLong(Constants.HEADER_SIZE, Constants.DEFAULT_SIZE);
            logger.info("limitRate: " + limitRate);
            logger.info("headerSize: " + headerSize);
        }
    }

    public static class Constants {
        public static long DEFAULT_RATE = 500L;
        public static long DEFAULT_SIZE = 16L;
        public static String HEADER_SIZE = "headerSize";
        public static String LIMIT_RATE = "limitRate";
    }

}


