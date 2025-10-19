package operations;

import model.Movie;
import util.DBConnection;
import util.DBConnection.DbType;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据库操作工具类（支持PostgreSQL/openGauss）
 * 新增4组SQL性能对比的执行方法，聚焦SQL语法差异的耗时统计
 */
public class SQLOperations {
    public static final String DATABASE_NAME = "filmdb";

    // 函数式接口复用（参数设置、结果处理）
    @FunctionalInterface
    private interface ParameterSetter {
        void setParameters(PreparedStatement stmt) throws SQLException;
    }

    @FunctionalInterface
    private interface ResultSetProcessor<T> {
        T process(ResultSet rs) throws SQLException;
    }

    /**
     * 关闭数据库资源（复用原有逻辑）
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
     * 解析执行时间（复用原有逻辑，两种数据库均支持"Execution Time:"标识）
     */
    private static double parseExecutionTime(ResultSet rs) throws SQLException {
        while (rs.next()) {
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                String line = rs.getString(i);
                if (line != null && line.contains("Execution Time:")) {
                    String timeStr = line.split("Execution Time:")[1].trim().split(" ")[0];
                    return Double.parseDouble(timeStr);
                } else if (line != null && line.contains("Total runtime:")) {
                    String timeStr = line.split("Total runtime:")[1].trim().split(" ")[0];
                    return Double.parseDouble(timeStr);
                }
            }
        }
        return 0;
    }

    /**
     * 通用EXPLAIN ANALYZE执行方法（复用原有逻辑，适配所有SQL）
     */
    private static double executeExplainAnalyzeQuery(DbType dbType, String sql, ParameterSetter parameterSetter) throws SQLException {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = DBConnection.getConnection(dbType, DATABASE_NAME);
            stmt = conn.prepareStatement(sql);
            if (parameterSetter != null) {
                parameterSetter.setParameters(stmt);
            }
            rs = stmt.executeQuery();
            return parseExecutionTime(rs);
        } finally {
            closeResources(rs, stmt, conn);
        }
    }

    // ========================== 1. 过滤逻辑对比 SQL ==========================
    /**
     * 1.1 子查询嵌套：先过滤country再过滤年份
     */
    public static double filterNested1(DbType dbType) throws SQLException {
        String sql = "EXPLAIN ANALYZE " +
                "select * from (" +
                "   select * from movies where country = 'us'" +
                ") as us_movies where year_released between 1940 and 1949;";
        return executeExplainAnalyzeQuery(dbType, sql, null);
    }

    /**
     * 1.2 子查询嵌套：先过滤年份再过滤country
     */
    public static double filterNested2(DbType dbType) throws SQLException {
        String sql = "EXPLAIN ANALYZE " +
                "select * from (" +
                "   select * from movies where year_released between 1940 and 1949" +
                ") as movies_from_the_1940s where country = 'us';";
        return executeExplainAnalyzeQuery(dbType, sql, null);
    }

    /**
     * 1.3 直接过滤：单条件组合
     */
    public static double filterDirect(DbType dbType) throws SQLException {
        String sql = "EXPLAIN ANALYZE " +
                "select * from movies where country = 'us' and year_released between 1940 and 1949;";
        return executeExplainAnalyzeQuery(dbType, sql, null);
    }

    // ========================== 2. UPPER函数对比 SQL ==========================
    /**
     * 2.1 双NOT LIKE：区分大小写过滤
     */
    public static double upperCompareNoFunc(DbType dbType) throws SQLException {
        String sql = "EXPLAIN ANALYZE " +
                "select * from movies where title not like '%A%' and title not like '%a%';";
        return executeExplainAnalyzeQuery(dbType, sql, null);
    }

    /**
     * 2.2 UPPER+NOT LIKE：统一大小写过滤
     */
    public static double upperCompareWithFunc(DbType dbType) throws SQLException {
        String sql = "EXPLAIN ANALYZE " +
                "select * from movies where upper(title) not like '%A%';";
        return executeExplainAnalyzeQuery(dbType, sql, null);
    }

    // ========================== 3. 集合运算 vs 连接 SQL ==========================
    /**
     * 3.1 集合运算：INTERSECT求交集
     */
    public static double setIntersect(DbType dbType) throws SQLException {
        String sql = "EXPLAIN ANALYZE " +
                "select country_code from countries intersect select distinct country from movies;";
        return executeExplainAnalyzeQuery(dbType, sql, null);
    }

    /**
     * 3.2 连接查询：JOIN求交集
     */
    public static double joinIntersect(DbType dbType) throws SQLException {
        String sql = "EXPLAIN ANALYZE " +
                "select c.country_code from countries c " +
                "join (select distinct country from movies) m on c.country_code = m.country;";
        return executeExplainAnalyzeQuery(dbType, sql, null);
    }

    /**
     * 3.3 集合运算：EXCEPT求差集
     */
    public static double setExcept(DbType dbType) throws SQLException {
        String sql = "EXPLAIN ANALYZE " +
                "select country_code from countries except select distinct country from movies;";
        return executeExplainAnalyzeQuery(dbType, sql, null);
    }

    /**
     * 3.4 连接查询：LEFT JOIN求差集
     */
    public static double joinExcept(DbType dbType) throws SQLException {
        String sql = "EXPLAIN ANALYZE " +
                "select c.country_code from countries c " +
                "left outer join (select distinct country from movies) m on c.country_code = m.country " +
                "where m.country is null;";
        return executeExplainAnalyzeQuery(dbType, sql, null);
    }

    // ========================== 4. 子查询类型对比 SQL ==========================
    /**
     * 4.1 相关子查询：EXISTS
     */
    public static double subqueryExists(DbType dbType) throws SQLException {
        String sql = "EXPLAIN ANALYZE " +
                "select distinct m.title from movies m " +
                "where exists( " +
                "   select null from credits c join people p on p.peopleid = c.peopleid " +
                "   where c.credited_as = 'A' and p.born >= 1970 and c.movieid = m.movieid " +
                ");";
        return executeExplainAnalyzeQuery(dbType, sql, null);
    }

    /**
     * 4.2 非相关子查询：IN
     */
    public static double subqueryIn(DbType dbType) throws SQLException {
        String sql = "EXPLAIN ANALYZE " +
                "select m.title from movies m " +
                "where m.movieid in ( " +
                "   select distinct c.movieid from credits c join people p on p.peopleid = c.peopleid " +
                "   where c.credited_as = 'A' and p.born >= 1970 " +
                ");";
        return executeExplainAnalyzeQuery(dbType, sql, null);
    }

    // ========================== 原有方法复用（保留加载数据能力） ==========================
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
}