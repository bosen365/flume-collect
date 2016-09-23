package cn.migu.flume.model;

/**
 * All rights Reserved, Designed by Migu.cn
 *
 * @Description: 资源监控信息
 * @Author: ChenYao
 * @Date: 2016/7/11 15:34
 * @Version: v1.0
 */
public class MonitorInfo {

    public static final String TABLE_NAME = "monitor_info";

    // 当前时间
    private String sys_time;
    // 主机
    private String host;
    // 用户空间占用cpu百分比
    private String cpu_userUse;
    // 内核空间占用cpu百分比
    private String cpu_sysUse;
    // 空闲cpu
    private String cpu_idle;
    // 总物理内存
    private String mem_total;
    // 已用内存
    private String mem_used;
    // 空闲内存总量
    private String mem_free;
    // 用于内核缓存的内存大小
    private String mem_buffCache;


    public String getSys_time() {
        return sys_time;
    }

    public void setSys_time(String sys_time) {
        this.sys_time = sys_time;
    }

    public String getCpu_userUse() {
        return cpu_userUse;
    }

    public void setCpu_userUse(String cpu_userUse) {
        this.cpu_userUse = cpu_userUse;
    }

    public String getCpu_sysUse() {
        return cpu_sysUse;
    }

    public void setCpu_sysUse(String cpu_sysUse) {
        this.cpu_sysUse = cpu_sysUse;
    }

    public String getCpu_idle() {
        return cpu_idle;
    }

    public void setCpu_idle(String cpu_idle) {
        this.cpu_idle = cpu_idle;
    }

    public String getMem_total() {
        return mem_total;
    }

    public void setMem_total(String mem_total) {
        this.mem_total = mem_total;
    }

    public String getMem_used() {
        return mem_used;
    }

    public void setMem_used(String mem_used) {
        this.mem_used = mem_used;
    }

    public String getMem_free() {
        return mem_free;
    }

    public void setMem_free(String mem_free) {
        this.mem_free = mem_free;
    }

    public String getMem_buffCache() {
        return mem_buffCache;
    }

    public void setMem_buffCache(String mem_buffCache) {
        this.mem_buffCache = mem_buffCache;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }


    @Override
    public String toString() {
        return "MonitorInfo{" +
                "sys_time='" + sys_time + '\'' +
                ", host='" + host + '\'' +
                ", cpu_userUse='" + cpu_userUse + '\'' +
                ", cpu_sysUse='" + cpu_sysUse + '\'' +
                ", cpu_idle='" + cpu_idle + '\'' +
                ", mem_total='" + mem_total + '\'' +
                ", mem_used='" + mem_used + '\'' +
                ", mem_free='" + mem_free + '\'' +
                ", mem_buffCache='" + mem_buffCache + '\'' +
                '}';
    }
}
