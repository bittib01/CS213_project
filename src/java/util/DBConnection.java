package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 数据库连接工具类
 *
 * <p>该类提供了灵活的数据库连接管理功能，主要特性包括：</p>
 *
 * <ul>
 *   <li>支持连接 PostgreSQL 及其兼容数据库（如 OpenGauss）</li>
 *   <li>可动态指定数据库名称及连接参数（主机、端口、用户名、密码）</li>
 *   <li>提供默认配置参数，简化常用场景下的连接操作</li>
 * </ul>
 *
 * <p>使用时通过指定数据库类型（ DbType ）和连接参数获取连接。</p>
 */
public class DBConnection {
    private static final String POSTGRESQL_DEFAULT_HOST = "localhost";
    private static final int POSTGRESQL_DEFAULT_PORT = 5432;
    public static final String POSTGRESQL_DEFAULT_USER = "postgres";
    private static final String POSTGRESQL_DEFAULT_PASS = "123456";

    private static final String OPENGAUSS_DEFAULT_HOST = "localhost";
    private static final int OPENGAUSS_DEFAULT_PORT = 5433;
    private static final String OPENGAUSS_DEFAULT_USER = "openGauss";
    private static final String OPENGAUSS_DEFAULT_PASS = "openGauss@123";

    /**
     * 数据库类型枚举
     */
    public enum DbType {
        POSTGRESQL,
        OPENGAUSS;

        public String toString() {
            return name().toLowerCase();
        }
    }

    /**
     * 根据数据库类型和名称获取连接（使用本类中设置的默认主机，端口，用户名，密码）
     *
     * @param dbType 数据库类型（ PostgreSQL/OpenGauss ）
     * @param dbName 要连接的具体数据库名称
     * @return 数据库连接对象
     * @throws SQLException 连接失败时抛出SQL异常
     */
    public static Connection getConnection(DbType dbType, String dbName) throws SQLException {
        String host;
        int port;
        String user;
        String pass;

        switch (dbType) {
            case POSTGRESQL:
                host = POSTGRESQL_DEFAULT_HOST;
                port = POSTGRESQL_DEFAULT_PORT;
                user = POSTGRESQL_DEFAULT_USER;
                pass = POSTGRESQL_DEFAULT_PASS;
                break;
            case OPENGAUSS:
                host = OPENGAUSS_DEFAULT_HOST;
                port = OPENGAUSS_DEFAULT_PORT;
                user = OPENGAUSS_DEFAULT_USER;
                pass = OPENGAUSS_DEFAULT_PASS;
                break;
            default:
                throw new IllegalArgumentException("不支持的数据库类型: " + dbType);
        }

        return getConnection(host, port, dbName, user, pass);
    }

    /**
     * 全参数连接方法（完全自定义连接参数）
     *
     * @param host 数据库主机地址（如 localhost 或 IP）
     * @param port 数据库端口号
     * @param dbName 要连接的数据库名称
     * @param user 登录用户名
     * @param pass 登录密码
     * @return 数据库连接对象
     * @throws SQLException 连接失败时抛出 SQL 异常
     */
    public static Connection getConnection(String host, int port,
                                           String dbName, String user, String pass) throws SQLException {
        String url = String.format("jdbc:postgresql://%s:%d/%s",
                host,
                port,
                dbName);
        return DriverManager.getConnection(url, user, pass);
    }

    /**
     * 获取默认数据库连接（ PostgreSQL + filmdb 数据库）
     *
     * @return 默认数据库连接对象
     * @throws SQLException 连接失败时抛出SQL异常
     */
    public static Connection getConnection() throws SQLException {
        return getConnection(DbType.POSTGRESQL, "postgres");
    }

    /**
     * 关闭数据库连接
     *
     * @param conn 需要关闭的连接对象（可为 null ）
     */
    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
