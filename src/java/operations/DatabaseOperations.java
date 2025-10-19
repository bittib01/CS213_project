package operations;

import model.Movie;
import util.DBConnection;
import util.DBConnection.DbType;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据库操作工具类
 * 提供电影数据的查询、更新等数据库操作，并记录执行时间
 */
public class DatabaseOperations {
    private static final int TITLE_MAX_LENGTH = 100;
    private static final String UPDATE_PATTERN_FROM = "to";
    private static final String UPDATE_PATTERN_TO = "ttoo";
    public static final String DATABASE_NAME = "movies_100000";

    /**
     * 函数式接口：用于设置 PreparedStatement 的参数
     */
    @FunctionalInterface
    private interface ParameterSetter {
        void setParameters(PreparedStatement stmt) throws SQLException;
    }

    /**
     * 函数式接口：用于处理 ResultSet 并返回结果
     * @param <T> 处理结果的类型
     */
    @FunctionalInterface
    private interface ResultSetProcessor<T> {
        T process(ResultSet rs) throws SQLException;
    }

    /**
     * 关闭数据库资源（ ResultSet、Statement、Connection ）
     * 统一处理资源关闭，避免重复代码
     * @param rs 结果集对象
     * @param stmt 语句对象
     * @param conn 连接对象
     */
    private static void closeResources(ResultSet rs, Statement stmt, Connection conn) {
        try {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            DBConnection.closeConnection(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 解析查询执行时间
     * <p>从 EXPLAIN ANALYZE 的结果中提取执行时间</p>
     *
     * @param rs 结果集
     * @return 执行时间（秒）
     * @throws SQLException SQL 异常
     */
    private static double parseExecutionTime(ResultSet rs) throws SQLException {
        while (rs.next()) {
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                String line = rs.getString(i);
                if (line != null && line.contains("Execution Time:")) {
                    String timeStr = line.split("Execution Time:")[1].trim().split(" ")[0];
                    return Double.parseDouble(timeStr);
                }
            }
        }
        return 0;
    }

    /**
     * 执行带 EXPLAIN ANALYZE 的查询并返回执行时间
     * <p>通用方法：处理连接获取、参数设置、查询执行和资源关闭</p>
     *
     * @param sql 要执行的 SQL 语句（包含 EXPLAIN ANALYZE ）
     * @param parameterSetter 参数设置器
     * @return 执行时间（秒）
     * @throws SQLException SQL 异常
     */
    private static double executeExplainAnalyzeQuery(DbType dbType, String sql, ParameterSetter parameterSetter) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBConnection.getConnection(dbType, DATABASE_NAME);
            stmt = conn.prepareStatement(sql);

            parameterSetter.setParameters(stmt);
            rs = stmt.executeQuery();

            return parseExecutionTime(rs);
        } finally {
            // 关闭资源
            closeResources(rs, stmt, conn);
        }
    }

    /**
     * 执行查询并处理结果集
     * <p>通用方法：处理连接获取、查询执行、结果处理和资源关闭</p>
     *
     * @param sql 要执行的 SQL 语句
     * @param processor 结果集处理器
     * @param <T> 处理结果的类型
     * @return 处理后的结果
     * @throws SQLException SQL 异常
     */
    private static <T> T executeQuery(DbType dbType, String sql, ResultSetProcessor<T> processor) throws SQLException {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBConnection.getConnection(dbType, DATABASE_NAME);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            return processor.process(rs);
        } finally {
            closeResources(rs, stmt, conn);
        }
    }

    /**
     * 模糊查询 - 搜索标题包含特定字符串的电影
     * @param keyword 搜索关键词
     * @return 执行时间（秒）
     * @throws SQLException SQL 异常
     */
    public static double fuzzySearch(DbType dbType, String keyword) throws SQLException {
        String sql = "EXPLAIN ANALYZE SELECT * FROM movies WHERE title LIKE ?";
        return executeExplainAnalyzeQuery(dbType, sql, stmt -> stmt.setString(1, "%" + keyword + "%"));
    }

    /**
     * 精确查询 - 根据电影 ID 查询
     * @param movieId 电影 ID
     * @return 执行时间（秒）
     * @throws SQLException SQL异常
     */
    public static double exactSearch(DbType dbType, int movieId) throws SQLException {
        String sql = "EXPLAIN ANALYZE SELECT * FROM movies WHERE movieid = ?";
        return executeExplainAnalyzeQuery(dbType, sql, stmt -> stmt.setInt(1, movieId));
    }

    /**
     * 范围查询 - 查询特定年份范围内的电影
     * @param startYear 起始年份
     * @param endYear 结束年份
     * @return 执行时间（秒）
     * @throws SQLException SQL异常
     */
    public static double rangeSearch(DbType dbType, int startYear, int endYear) throws SQLException {
        String sql = "EXPLAIN ANALYZE SELECT * FROM movies WHERE year_released BETWEEN ? AND ?";
        return executeExplainAnalyzeQuery(dbType, sql, stmt -> {
            stmt.setInt(1, startYear);
            stmt.setInt(2, endYear);
        });
    }

    /**
     * 更新操作 - 将所有标题中的"to"替换为"ttoo"（确保不超过长度限制）
     * @return 执行时间（秒）
     * @throws SQLException SQL异常
     */
    public static double updateTitles(DbType dbType) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        double executionTime = -1;

        try {
            conn = DBConnection.getConnection(dbType, DATABASE_NAME);
            conn.setAutoCommit(false);

            String updateSql = String.format(
                    "UPDATE movies " +
                            "SET title = REPLACE(title, '%s', '%s') " +
                            "WHERE length(REPLACE(title, '%s', '%s')) <= %d",
                    UPDATE_PATTERN_FROM, UPDATE_PATTERN_TO,
                    UPDATE_PATTERN_FROM, UPDATE_PATTERN_TO,
                    TITLE_MAX_LENGTH
            );

            String explainSql = "EXPLAIN ANALYZE " + updateSql;
            rs = conn.prepareStatement(explainSql).executeQuery();
            executionTime = parseExecutionTime(rs);

            stmt = conn.prepareStatement(updateSql);
            stmt.executeUpdate();
            conn.commit();

        } catch (SQLException e) {
            if (conn != null) {
                conn.rollback();
            }
            throw e;
        } finally {
            closeResources(rs, stmt, conn);
        }

        return executionTime;
    }

    /**
     * 从数据库加载所有电影数据到内存
     *
     * @return 电影对象列表
     * @throws SQLException SQL异常
     */
    public static List<Movie> loadAllMovies(DbType dbType) throws SQLException {
        String sql = "SELECT * FROM movies";
        return executeQuery(dbType, sql, rs -> {
            List<Movie> movies = new ArrayList<>();
            while (rs.next()) {
                Movie movie = new Movie(
                        rs.getInt("movieid"),
                        rs.getString("title"),
                        rs.getString("country"),
                        rs.getInt("year_released"),
                        rs.getInt("runtime")
                );
                movies.add(movie);
            }
            return movies;
        });
    }
}
