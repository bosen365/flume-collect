package cn.migu.flume.configuration;


/**
 * All rights Reserved, Designed by Migu.cn
 *
 * @Description: 心跳相关配置
 * @Author : ChenYao
 * @Version : v1.0
 * @Date : 2016/7/1 11:25
 */
public class HeartBeatConstants {

    // host 主机名
    public static final String HOST_HEADER = "hostHeader";

    // agent name
    public static final String AGENT_CONF_NAME = "agent_name";
    public static final String AGENT_CONF_DEFAULT = "NULL";

    // 心跳发送时间间隔 单位：秒
    public static final String HB_INTERVAL_CONF_NAME = "hb_interval_seconds";
    public static final int HB_INTERVAL_DEFAULT = 10;

    // 监控信息获取频率
    public static final String MT_INTERVAL_CONF_NAME = "monitor_interval_seconds";
    public static final int MT_INTERVAL_DEFAULT = 10;

    // 是否打开监听
    public static final String OPEN_MONITOR_KEY = "open_monitor";
    // 默认关闭监听
    public static final boolean OPEN_MONITOR_DEFAULT = false;


    // header 标记:监控信息, 需要 sink 对应
    public static final String MONITOR_INFO_HEADER_TAG = "TAG-MonitorInfo";
    // header 标记:心跳信息，需要 sink 对应
    public static final String HEART_BEAT_HEADER_TAG = "TAG-HeartBeat";

    // hb send time
    public static final String SEND_TIME_QUALIFY = "send_time";

    // 开始时间
    public static final String START_TIME_QUALIFY = "start_time";

}
