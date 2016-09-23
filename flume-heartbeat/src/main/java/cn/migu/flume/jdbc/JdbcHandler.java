package cn.migu.flume.jdbc;

import com.mysql.jdbc.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

/**
 * All rights Reserved, Designed by Migu.cn
 *
 * @Description: jdbc handler
 * @Author : ChenYao
 * @Version : v1.0
 * @Date : 2016/8/1 11:25
 */
public class JdbcHandler {

    public static final Logger LOG = LoggerFactory.getLogger(JdbcHandler.class);

    public static final String MYSQL_CONF_FILE_NAME = "mysql.properties";
    public static final String MYSQL_URL_CONF_NAME = "mysql.url";
    public static final String MYSQL_USER_CONF_NAME = "mysql.user";
    public static final String MYSQL_PASSWORD_CONF_NAME = "mysql.password";

    // mysql connect url
    public static String mysql_url;
    // mysql user name
    public static String mysql_user;
    // mysql user password
    public static String mysql_password;

    private static JdbcTemplate jdbcTemplate;

    private static void init() {
        Properties p = new Properties();
        InputStream in = JdbcHandler.class.getClassLoader().getResourceAsStream(MYSQL_CONF_FILE_NAME);
        if (null != in) {
            try {
                p.load(in);
                mysql_url = p.getProperty(MYSQL_URL_CONF_NAME);
                mysql_user = p.getProperty(MYSQL_USER_CONF_NAME);
                mysql_password = p.getProperty(MYSQL_PASSWORD_CONF_NAME);
            } catch (IOException e) {
                LOG.error("error occur when read config file {}", MYSQL_CONF_FILE_NAME, e);
            } finally {
                try {
                    in.close();
                } catch (IOException e) {
                    LOG.error("", e);
                }
            }
        } else {
            LOG.error("InputStream is null");
        }
    }

    public static JdbcTemplate getHandler() throws SQLException {
        if (jdbcTemplate == null) {
            synchronized (JdbcHandler.class) {
                if (jdbcTemplate == null) {
                    init();
                    SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
                    dataSource.setDriver(new Driver());
                    dataSource.setUrl(mysql_url);
                    dataSource.setUsername(mysql_user);
                    dataSource.setPassword(mysql_password);
                    jdbcTemplate = new JdbcTemplate(dataSource);
                }
            }
        }
        return jdbcTemplate;
    }
}
