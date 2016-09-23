package cn.migu.flume.helper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * All rights Reserved, Designed by Migu.cn
 *
 * @Description: linux top 信息获取
 * @Author : ChenYao
 * @Version : v1.0
 * @Date : 2016/8/1 11:25
 */
public class SystemMonitor {

    // 获取top信息 linux command
    private static final String TOP_COMMAND = "top -b -n 1";
    // 取 top 信息行数(前几行)
    private static final int LINES = 5;
    // 拼接每行信息所用分隔符
    private static final String SEP = "#";

    public static String getTopInfo() throws IOException {
        Runtime r = Runtime.getRuntime();
        Process pro = r.exec(TOP_COMMAND);
        BufferedReader in1 = new BufferedReader(new InputStreamReader(pro.getInputStream()));
        String line;
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < LINES; i++) {
            line = in1.readLine();
            if (line == null) return "";
            sb.append(line);
            if (i != LINES - 1) {
                sb.append(SEP);
            }
        }

        in1.close();
        return sb.toString();
    }
}
