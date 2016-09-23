package cn.migu.flume.sink;

import cn.migu.flume.jdbc.JdbcHandler;
import cn.migu.flume.model.AgentStatus;
import cn.migu.flume.model.MonitorInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cn.migu.flume.configuration.HeartBeatConstants.*;

/**
 * All rights Reserved, Designed by Migu.cn
 *
 * @Description: 心跳接收端sink
 * @Author : ChenYao
 * @Version : v1.0
 * @Date : 2016/8/1 11:25
 */
public class HeartBeatSink extends AbstractSink implements Configurable {

    public static final Logger logger = LoggerFactory.getLogger(HeartBeatSink.class);
    private static final String SEP = "#";
    private static final int TOP_LINES = 5;
    private static final String pattern = ".*(\\d{2}:\\d{2}:\\d{2}).*";
    private static final Pattern p = Pattern.compile(pattern);

    private String hostKey;

    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;
        try {
            transaction.begin();
            event = channel.take();
            if (event != null) {
                if (isHeartBeatEvent(event)) {
                    logger.info("is heart>>>>>");
                    storeHeartBeatData(event);
                } else if (isMonitorEvent(event)) {
                    logger.info("is mt >>>>>");
                    storeMonitorInfo(event);
                }
            } else {
                result = Status.BACKOFF;
            }
            transaction.commit();
        } catch (Exception ex) {
            transaction.rollback();
            result = Status.BACKOFF;
            logger.warn("error", ex);
        } finally {
            transaction.close();
        }

        return result;
    }


    public static void main(String[] args) {
        String sendTime = String.valueOf(System.currentTimeMillis());
        String date = DateFormatUtils.format(Long.parseLong(sendTime), "yyyy-MM-dd");
        System.out.println(date);
        String test = "fds 12:21 fds /";
        test = test.replaceAll("[a-zA-Z/]", "");
        System.out.println(test);
    }


    /**
     * 持久化监控信息
     *
     * @param event event
     */
    private void storeMonitorInfo(Event event) {
        String body = new String(event.getBody());
        Map<String, String> header = event.getHeaders();
        String sendTime = header.get(SEND_TIME_QUALIFY);
        String host = header.get(hostKey);

        // 解析 top body
        if (!StringUtils.isBlank(body) && body.split(SEP).length == TOP_LINES) {
            try {
                MonitorInfo mi = new MonitorInfo();
                mi.setHost(host);

                String[] list = body.split(SEP);
                String basicInfo = list[0];
                String time = getTime(basicInfo);

                // get day of year
                String date = DateFormatUtils.format(Long.parseLong(sendTime), "yyyy-MM-dd");
                mi.setSys_time(date + " " + time);

                String memInfos = list[3];
                String[] mems = getStrings(memInfos);
                mi.setMem_total(mems[0]);
                mi.setMem_free(mems[1]);
                mi.setMem_used(mems[2]);
                mi.setMem_buffCache(mems[3]);

                String cpuInfos = list[2];
                String[] cpus = getStrings(cpuInfos);
                mi.setCpu_userUse(cpus[0]);
                mi.setCpu_sysUse(cpus[1]);
                mi.setCpu_idle(cpus[3]);

                updateMonitorInfo(mi);
                logger.info("Monitor Info: {}", mi.toString());
            } catch (Exception e) {
                logger.error("extract top info error, just skip", e);
            }
        }
    }


    /**
     * 更新监控信息
     *
     * @param mi
     */
    private void updateMonitorInfo(MonitorInfo mi) {
        try {
            JdbcTemplate handler = JdbcHandler.getHandler();
            String sql = "insert into " + MonitorInfo.TABLE_NAME
                    + "(id,sys_time,cpu_userUse,cpu_sysUse,cpu_idle," +
                    "mem_total,mem_free,mem_used,mem_bufferCache) values(?,?,?,?,?,?,?,?,?)";
            handler.update(sql,
                    UUID.randomUUID().toString(),
                    mi.getSys_time(),
                    mi.getCpu_userUse(),
                    mi.getCpu_sysUse(),
                    mi.getCpu_idle(),
                    mi.getMem_total(),
                    mi.getMem_free(),
                    mi.getMem_used(),
                    mi.getMem_buffCache()
            );
        } catch (SQLException e) {
            logger.error("sql exception", e);
        }
    }


    // 从top第一行获取时间,格式: 23:11:32
    private String getTime(String exp) {
        Matcher m = p.matcher(exp);
        if (m.find()) {
            return m.group(1);
        }
        return "";
    }

    // top信息去掉 所有字母与/ 取关键信息
    private String[] getStrings(String str) {
        return str.replaceAll("[A-Za-z/ ]", "").split(":")[1].split(",");
    }

    // 是监控信息
    private boolean isMonitorEvent(Event event) {
        Map<String, String> header = event.getHeaders();
        return header != null && header.containsKey(MONITOR_INFO_HEADER_TAG);
    }

    /**
     * 是心跳信息
     *
     * @param event event
     * @return true if is heartbeat
     */
    private boolean isHeartBeatEvent(Event event) {
        Map<String, String> header = event.getHeaders();
        return header != null && header.containsKey(HEART_BEAT_HEADER_TAG);
    }

    /**
     * 持久化心跳信息至mysql
     *
     * @param event event
     */
    private void storeHeartBeatData(Event event) {
        Map<String, String> header = event.getHeaders();
        String host = header.get(hostKey);
        String agent_name = header.get(AGENT_CONF_NAME);
        String send_time = header.get(SEND_TIME_QUALIFY);
        String start_time = header.get(START_TIME_QUALIFY);

        logger.info("host: {}", host);
        logger.info("agent_name: {}", agent_name);
        logger.info("send_time: {}", send_time);
        logger.info("start_time: {}", start_time);

        // store to mysql
        // | hostKey | agent_name | start_time | latest_heartbeat |
        updateHeartBeat(host, agent_name, send_time, start_time);
    }

    /**
     * 更新心跳信息
     *
     * @param host       host
     * @param agent_name agent name
     * @param send_time  发送时间
     * @param start_time agent 启动时间
     */
    private void updateHeartBeat(String host, String agent_name, String send_time, String start_time) {
        try {
            JdbcTemplate jt = JdbcHandler.getHandler();
            if (exist(host, agent_name, start_time)) {
                // update
                String updateSql = "update " + AgentStatus.TABLE_NAME +
                        " set latest_heartbeat=? where host = ? and agent_name = ? and start_time = ?";
                jt.update(updateSql, send_time, host, agent_name, start_time);
            } else {
                // insert new record
                String insertSql = "insert into "
                        + AgentStatus.TABLE_NAME + " (id, host,agent_name,start_time,latest_heartbeat) values(?,?,?,?)";
                jt.update(insertSql, UUID.randomUUID().toString(), host, agent_name, start_time, send_time);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 是否存在心跳数据
     *
     * @param host       host
     * @param agent_name agent name
     * @param start_time start time
     *
     * @return true if exist heartbeat data for host agent
     * @throws SQLException
     */
    private boolean exist(String host, String agent_name, String start_time) throws SQLException {
        String sql = "select count(*) from " + AgentStatus.TABLE_NAME + " where host = ? and agent_name = ? and start_time = ?";
        JdbcTemplate jt = JdbcHandler.getHandler();
        Integer r = jt.queryForObject(sql, Integer.class, host, agent_name, start_time);
        return r != null && r > 0;
    }

    public void configure(Context context) {
        hostKey = context.getString(HOST_HEADER, "host");
    }


    public void start() {
        super.start();
    }
}
