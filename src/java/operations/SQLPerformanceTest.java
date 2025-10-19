package operations;

import util.DBConnection.DbType;
import util.Docker;

import java.io.IOException;
import java.sql.SQLException;

/**
 * PostgreSQL与openGauss的SQL语法性能对比测试类
 * 聚焦4组目标SQL的耗时对比，支持单数据库测试或双数据库对比
 */
public class SQLPerformanceTest {
    /** 测试轮次（建议≥5轮，减少偶然误差） */
    private static final int TEST_RUNS = 3;
    /** 测试模式：选择要对比的数据库类型 */
    public static final TestDbMode SELECTED_MODE = TestDbMode.BOTH;
    /** Docker操作实例（动态适配数据库类型） */
    private static Docker docker;

    /**
     * 数据库测试模式枚举
     */
    public enum TestDbMode {
        POSTGRESQL_ONLY,  // 仅测试PostgreSQL
        OPENGAUSS_ONLY,   // 仅测试openGauss
        BOTH              // 同时测试两种数据库（对比跨库差异）
    }

    public static void main(String[] args) {
        try {
            // 1. 初始化测试环境（启动对应数据库容器）
            initTestEnv();

            // 2. 执行4组SQL性能测试
            runFilterCompareTest();    // 测试1：过滤逻辑对比
            runUpperCompareTest();     // 测试2：UPPER函数对比
            runSetJoinCompareTest();   // 测试3：集合运算vs连接
            runSubqueryCompareTest();  // 测试4：子查询类型对比

            // 3. 清理资源
            stopDockerContainer();
            System.out.printf("%n✅ 所有SQL性能测试执行完成，资源已清理%n");

        } catch (SQLException | InterruptedException e) {
            System.err.printf("❌ 测试异常终止：%s%n", e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 初始化测试环境：启动对应数据库的Docker容器
     */
    private static void initTestEnv() throws InterruptedException, IOException {
        System.out.println("=== 🚀 初始化SQL性能测试环境 ===");
        switch (SELECTED_MODE) {
            case POSTGRESQL_ONLY:
                docker = new Docker(DbType.POSTGRESQL.toString());
                startDockerIfNotRunning(DbType.POSTGRESQL);
                break;
            case OPENGAUSS_ONLY:
                docker = new Docker(DbType.OPENGAUSS.toString());
                startDockerIfNotRunning(DbType.OPENGAUSS);
                break;
            case BOTH:
                // 启动PostgreSQL容器
                docker = new Docker(DbType.POSTGRESQL.toString());
                startDockerIfNotRunning(DbType.POSTGRESQL);
                // 启动openGauss容器（需确保Docker支持多容器，或复用同一容器实例动态切换）
                docker = new Docker(DbType.OPENGAUSS.toString());
                startDockerIfNotRunning(DbType.OPENGAUSS);
                break;
        }
    }

    /**
     * 启动Docker容器（若未运行）
     */
    private static void startDockerIfNotRunning(DbType dbType) throws InterruptedException, IOException {
        if (!docker.isContainerRunning()) {
            System.out.printf("启动%s容器...%n", dbType);
            docker.startContainer();
        }
        System.out.printf("%s容器状态：✅ 运行中%n", dbType);
    }

    /**
     * 测试1：过滤逻辑对比（3种SQL写法）
     */
    private static void runFilterCompareTest() throws SQLException, InterruptedException, IOException {
        System.out.printf("%n=== 📊 测试1：过滤逻辑对比（共%d轮） ===", TEST_RUNS);
        String testDesc = "SQL1:先过滤country再过滤年份 | SQL2:先过滤年份再过滤country | SQL3:直接组合条件";

        for (int i = 1; i <= TEST_RUNS; i++) {
            System.out.printf("%n第%d轮：%s%n", i, testDesc);
            executeAndPrint(DbType.POSTGRESQL, "PostgreSQL",
                    SQLOperations::filterNested1,
                    SQLOperations::filterNested2,
                    SQLOperations::filterDirect);

            if (SELECTED_MODE == TestDbMode.BOTH) {
                executeAndPrint(DbType.OPENGAUSS, "openGauss",
                        SQLOperations::filterNested1,
                        SQLOperations::filterNested2,
                        SQLOperations::filterDirect);
            }
            resetDbState(); // 重置数据库状态（避免缓存影响下一轮）
            Thread.sleep(5000);
        }
    }

    /**
     * 测试2：UPPER函数对比（2种SQL写法）
     */
    private static void runUpperCompareTest() throws SQLException, InterruptedException, IOException {
        System.out.printf("%n=== 📊 测试2：UPPER函数对比（共%d轮） ===", TEST_RUNS);
        String testDesc = "SQL1:双NOT LIKE（区分大小写） | SQL2:UPPER+NOT LIKE（统一大小写）";

        for (int i = 1; i <= TEST_RUNS; i++) {
            System.out.printf("%n第%d轮：%s%n", i, testDesc);
            executeAndPrint(DbType.POSTGRESQL, "PostgreSQL",
                    SQLOperations::upperCompareNoFunc,
                    SQLOperations::upperCompareWithFunc);

            if (SELECTED_MODE == TestDbMode.BOTH) {
                executeAndPrint(DbType.OPENGAUSS, "openGauss",
                        SQLOperations::upperCompareNoFunc,
                        SQLOperations::upperCompareWithFunc);
            }
            resetDbState(); // 重置数据库状态（避免缓存影响下一轮）
            Thread.sleep(5000);
        }
    }

    /**
     * 测试3：集合运算 vs 连接（4种SQL写法）
     */
    private static void runSetJoinCompareTest() throws SQLException, InterruptedException, IOException {
        System.out.printf("%n=== 📊 测试3：集合运算vs连接（共%d轮） ===", TEST_RUNS);
        String testDesc1 = "SQL1:INTERSECT求交集 | SQL2:JOIN求交集";
        String testDesc2 = "SQL3:EXCEPT求差集 | SQL4:LEFT JOIN求差集";

        for (int i = 1; i <= TEST_RUNS; i++) {
            System.out.printf("%n第%d轮（交集对比）：%s%n", i, testDesc1);
            executeAndPrint(DbType.POSTGRESQL, "PostgreSQL",
                    SQLOperations::setIntersect,
                    SQLOperations::joinIntersect);

            System.out.printf("第%d轮（差集对比）：%s%n", i, testDesc2);
            executeAndPrint(DbType.POSTGRESQL, "PostgreSQL",
                    SQLOperations::setExcept,
                    SQLOperations::joinExcept);

            if (SELECTED_MODE == TestDbMode.BOTH) {
                System.out.printf("第%d轮（交集对比-OpenGauss）：%s%n", i, testDesc1);
                executeAndPrint(DbType.OPENGAUSS, "openGauss",
                        SQLOperations::setIntersect,
                        SQLOperations::joinIntersect);

                System.out.printf("第%d轮（差集对比-OpenGauss）：%s%n", i, testDesc2);
                executeAndPrint(DbType.OPENGAUSS, "openGauss",
                        SQLOperations::setExcept,
                        SQLOperations::joinExcept);
            }
            resetDbState(); // 重置数据库状态（避免缓存影响下一轮）
            Thread.sleep(5000);
        }
    }

    /**
     * 测试4：子查询类型对比（2种SQL写法）
     */
    private static void runSubqueryCompareTest() throws SQLException, InterruptedException, IOException {
        System.out.printf("%n=== 📊 测试4：子查询类型对比（共%d轮） ===", TEST_RUNS);
        String testDesc = "SQL1:EXISTS相关子查询 | SQL2:IN非相关子查询";

        for (int i = 1; i <= TEST_RUNS; i++) {
            System.out.printf("%n第%d轮：%s%n", i, testDesc);
            executeAndPrint(DbType.POSTGRESQL, "PostgreSQL",
                    SQLOperations::subqueryExists,
                    SQLOperations::subqueryIn);

            if (SELECTED_MODE == TestDbMode.BOTH) {
                executeAndPrint(DbType.OPENGAUSS, "openGauss",
                        SQLOperations::subqueryExists,
                        SQLOperations::subqueryIn);
            }
            resetDbState(); // 重置数据库状态（避免缓存影响下一轮）
            Thread.sleep(5000);
        }
    }

    /**
     * 执行SQL并打印耗时（支持多SQL批量对比）
     * @param dbType 数据库类型
     * @param dbName 数据库名称（用于日志输出）
     * @param sqlOps 要执行的SQL操作（可变参数，支持多个SQL对比）
     */
    private static void executeAndPrint(DbType dbType, String dbName, SqlOperation... sqlOps) throws SQLException, IOException, InterruptedException {
        for (int i = 0; i < sqlOps.length; i++) {
            resetDbState();
            Thread.sleep(5000);
            double costMs = sqlOps[i].execute(dbType);
            System.out.printf("  %s - SQL%d: %.3f ms%n", dbName, (i + 1), costMs);
        }
    }

    /**
     * 重置数据库状态（重启容器+清理缓存，确保每轮测试环境一致）
     */
    private static void resetDbState() throws InterruptedException, SQLException, IOException {
        switch (SELECTED_MODE) {
            case POSTGRESQL_ONLY:
                docker = new Docker(DbType.POSTGRESQL.toString());
                docker.restartContainer();
                break;
            case OPENGAUSS_ONLY:
                docker = new Docker(DbType.OPENGAUSS.toString());
                docker.restartContainer();
                break;
            case BOTH:
                docker = new Docker(DbType.POSTGRESQL.toString());
                docker.restartContainer();
                docker = new Docker(DbType.OPENGAUSS.toString());
                docker.restartContainer();
                break;
        }
    }

    /**
     * 停止Docker容器（测试结束后清理）
     */
    private static void stopDockerContainer() throws InterruptedException, IOException {
        switch (SELECTED_MODE) {
            case POSTGRESQL_ONLY:
                docker = new Docker(DbType.POSTGRESQL.toString());
                docker.stopContainer();
                break;
            case OPENGAUSS_ONLY:
                docker = new Docker(DbType.OPENGAUSS.toString());
                docker.stopContainer();
                break;
            case BOTH:
                docker = new Docker(DbType.POSTGRESQL.toString());
                docker.stopContainer();
                docker = new Docker(DbType.OPENGAUSS.toString());
                docker.stopContainer();
                break;
        }
    }

    /**
     * 函数式接口：封装单参数（DbType）的SQL执行操作
     */
    @FunctionalInterface
    private interface SqlOperation {
        double execute(DbType dbType) throws SQLException;
    }
}