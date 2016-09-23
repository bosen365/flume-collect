package cn.migu.flume.source;

import cn.migu.flume.helper.SystemMonitor;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static cn.migu.flume.configuration.HeartBeatConstants.*;


/**
 * All rights Reserved, Designed by Migu.cn
 *
 * @Description: 发送心跳与物理资源监控信息, 心跳的发送频率与监控信息的发送频率独立
 * @Author : ChenYao
 * @Version : v1.0
 * @Date : 2016/8/1 11:25
 */
public class HeartBeatSource extends AbstractSource implements EventDrivenSource,
        Configurable {

    private static final Logger logger = LoggerFactory.getLogger(HeartBeatSource.class);

    // 心跳event
    private static Event heartBeatEvent;
    // 心跳频率
    private int hb_interval_seconds;
    // 监控频率
    private int mt_interval_seconds;
    // agent 名
    private String agent_name;
    private ExecutorService executor;
    private Runner runner;

    // 是否发送监控信息 默认关闭
    private boolean openMonitor;


    /**
     * 配置加载，启动前首先读取配置文件
     *
     * @param context 默认 context
     */
    @Override
    public void configure(Context context) {
        hb_interval_seconds = context.getInteger(HB_INTERVAL_CONF_NAME, HB_INTERVAL_DEFAULT);
        agent_name = context.getString(AGENT_CONF_NAME, AGENT_CONF_DEFAULT);
        mt_interval_seconds = context.getInteger(MT_INTERVAL_CONF_NAME, MT_INTERVAL_DEFAULT);
        openMonitor = context.getBoolean(OPEN_MONITOR_KEY, OPEN_MONITOR_DEFAULT);

        logger.info("--------------- Monitor conf ----------------");
        logger.info("System info monitor status: {}", openMonitor ? "is open" : "is closed");
        logger.info("monitor agent name: {}", agent_name);
        logger.info("send heartBeat_interval : {} seconds", hb_interval_seconds);
        logger.info("send monitor info interval: {} seconds", mt_interval_seconds);
        logger.info("--------------- Monitor conf ----------------");

        setHeartBeatEvent();
    }

    /**
     * 构建心跳 event
     */
    private void setHeartBeatEvent() {
        Map<String, String> heartBeatHeader = new HashMap<>();
        heartBeatHeader.put(HEART_BEAT_HEADER_TAG, "");
        heartBeatHeader.put(AGENT_CONF_NAME, agent_name);
        heartBeatHeader.put(HB_INTERVAL_CONF_NAME, "" + hb_interval_seconds);
        heartBeatHeader.put(START_TIME_QUALIFY, "" + System.currentTimeMillis());
        heartBeatEvent = EventBuilder.withBody("".getBytes(), heartBeatHeader);
    }


    @Override
    public synchronized void start() {
        logger.debug("heart beat source start, send heartBeat_interval : {} s", hb_interval_seconds);
        executor = Executors.newSingleThreadExecutor();
        runner = new Runner(getChannelProcessor(), hb_interval_seconds, mt_interval_seconds, openMonitor);
        executor.submit(runner);
    }

    @Override
    public synchronized void stop() {
        super.stop();
    }

    private static class Runner implements Runnable {

        private ChannelProcessor cp;
        private int heartBeat_interval;
        private int monitor_interval;
        private boolean openMonitor;

        public Runner(ChannelProcessor cp, int heartBeat_interval, int monitor_interval, boolean openMonitor) {
            this.cp = cp;
            this.openMonitor = openMonitor;
            this.heartBeat_interval = heartBeat_interval;
            this.monitor_interval = monitor_interval;
        }

        public void run() {
            // 心跳服务
            ScheduledExecutorService heartBeatService = Executors.newSingleThreadScheduledExecutor();
            heartBeatService.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    heartBeatEvent.getHeaders().put(SEND_TIME_QUALIFY, System.currentTimeMillis() + "");
                    cp.processEvent(heartBeatEvent);
                }
            }, 0, heartBeat_interval, TimeUnit.SECONDS);

            // 监控服务
            if (openMonitor) {
                ScheduledExecutorService MonitorService = Executors.newSingleThreadScheduledExecutor();
                MonitorService.scheduleAtFixedRate(new Runnable() {
                    public void run() {
                        cp.processEvent(getMonitorEvent());
                    }
                }, 0, monitor_interval, TimeUnit.SECONDS);
            } else {
                logger.info("will not monitor");
            }
        }


        // 监控信息
        private Event getMonitorEvent() {

            // header
            // 标记是监控信息 放 header
            Map<String, String> header = new HashMap<>();
            header.put(MONITOR_INFO_HEADER_TAG, "");
            header.put(SEND_TIME_QUALIFY, String.valueOf(System.currentTimeMillis()));

            // body 默认为空，可能因为异常等原因没获取到数据
            String body = "";
            try {
                body = SystemMonitor.getTopInfo();
                logger.info("monitor body : {}", body);
            } catch (IOException e) {
                logger.warn(e.getMessage(), e);
            }

            return EventBuilder.withBody(body.getBytes(), header);
        }
    }
}
