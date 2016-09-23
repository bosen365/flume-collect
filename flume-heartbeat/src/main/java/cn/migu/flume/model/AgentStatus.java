package cn.migu.flume.model;

/**
 * All rights Reserved, Designed by Migu.cn
 *
 * @Description: agent 状态实体
 * @Author : ChenYao
 * @Version : v1.0
 * @Date : 2016/8/1 11:25
 */
public class AgentStatus {

    public static final String TABLE_NAME = "agent_status";

    // 主机名
    private String host;
    // agent name
    private String agent_name;
    // agent 启动时间
    private String start_time;
    // 最近一次心跳时间
    private String latest_heartbeat;


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getAgent_name() {
        return agent_name;
    }

    public void setAgent_name(String agent_name) {
        this.agent_name = agent_name;
    }

    public String getStart_time() {
        return start_time;
    }

    public void setStart_time(String start_time) {
        this.start_time = start_time;
    }

    public String getLatest_heartbeat() {
        return latest_heartbeat;
    }

    public void setLatest_heartbeat(String latest_heartbeat) {
        this.latest_heartbeat = latest_heartbeat;
    }
}
