package operations;

import model.Movie;
import util.DBConnection;
import util.Docker;
import util.DBConnection.DbType;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static operations.DatabaseOperations.DATABASE_NAME;
import static util.DBConnection.POSTGRESQL_DEFAULT_USER;

/**
 * 数据库、内存、文件电影操作性能对比测试主类
 *
 * <p>支持三种测试模式：数据库 vs 内存、数据库 vs 文件、单类型测试（仅数据库/仅内存/仅文件），
 * 可通过 {@link #selectedMode} 和 {@link #selectedSingleType} 配置运行时模式，
 * 核心优势是灵活对比不同存储方式的性能，同时保持环境一致性与数据隔离性。</p>
 */
public class PerformanceTest {
    /** 测试轮次 */
    private static final int TEST_RUNS = 20;
    /** 模糊查询关键词 */
    private static final String FUZZY_KEYWORD = "the";
    /** 精确查询目标电影 ID（需确保数据源中存在该 ID） */
    private static final int EXACT_MOVIE_ID = 1000;
    /** 范围查询起始年份（包含） */
    private static final int RANGE_START_YEAR = 1990;
    /** 范围查询结束年份（包含） */
    private static final int RANGE_END_YEAR = 2000;
    /** 默认测试数据库名称 */
    private static final String DEFAULT_DB_NAME = DATABASE_NAME;
    /** 默认数据库用户名 */
    private static final String DEFAULT_DB_USER = POSTGRESQL_DEFAULT_USER;
    /** 默认数据库类型 */
    private static final DbType DEFAULT_DB_TYPE = DbType.POSTGRESQL;
    /** Docker操作实例 */
    private static final Docker DOCKER = new Docker(DEFAULT_DB_TYPE.toString());

    /**
     * 测试模式枚举（运行时可通过修改该变量切换模式）
     * <ul>
     *   <li>DB_VS_MEMORY：数据库与内存操作对比</li>
     *   <li>DB_VS_FILE：数据库与文件操作对比</li>
     *   <li>SINGLE：单类型测试（需配合{@link #selectedSingleType}指定类型）</li>
     * </ul>
     */
    public static final TestMode selectedMode = TestMode.DB_VS_MEMORY;

    /**
     * 单类型测试的目标类型（仅当{@link #selectedMode}为 SINGLE 时生效）
     */
    public static final SingleTestType selectedSingleType = SingleTestType.FILE;

    /**
     * 测试模式枚举定义
     */
    public enum TestMode {
        DB_VS_MEMORY, DB_VS_FILE, SINGLE
    }

    /**
     * 单类型测试的目标类型枚举
     */
    public enum SingleTestType {
        DB, MEMORY, FILE
    }

    /**
     * 程序入口，触发性能测试流程（根据{@link #selectedMode}执行对应测试）
     * @param args 命令行参数（预留扩展：如通过参数指定测试模式，当前未启用）
     */
    public static void main(String[] args) {
        try {
            List<Movie> baseMovieList;
            if (selectedMode == TestMode.DB_VS_MEMORY || (selectedMode == TestMode.SINGLE && selectedSingleType == SingleTestType.MEMORY)) {
                baseMovieList = DatabaseOperations.loadAllMovies(DEFAULT_DB_TYPE);
                System.out.printf("✅ 内存操作基准数据加载完成：共读取 %d 条电影记录%n", baseMovieList.size());
            } else {
                baseMovieList = new ArrayList<>();
            }
            System.out.printf("✅ 文件操作路径配置完成：%s%n", FileOperations.MOVIE_FILE_PATH);

            runQueryTest("模糊查询（标题含关键词'" + FUZZY_KEYWORD + "'）",
                    () -> DatabaseOperations.fuzzySearch(DEFAULT_DB_TYPE, FUZZY_KEYWORD),
                    () -> InMemoryOperations.fuzzySearch(baseMovieList, FUZZY_KEYWORD),
                    () -> FileOperations.fuzzySearch(FUZZY_KEYWORD));

            runQueryTest("精确查询（电影ID=" + EXACT_MOVIE_ID + "）",
                    () -> DatabaseOperations.exactSearch(DEFAULT_DB_TYPE, EXACT_MOVIE_ID),
                    () -> InMemoryOperations.exactSearch(baseMovieList, EXACT_MOVIE_ID),
                    () -> FileOperations.exactSearch(EXACT_MOVIE_ID));

            runQueryTest("范围查询（年份" + RANGE_START_YEAR + "-" + RANGE_END_YEAR + "）",
                    () -> DatabaseOperations.rangeSearch(DEFAULT_DB_TYPE, RANGE_START_YEAR, RANGE_END_YEAR),
                    () -> InMemoryOperations.rangeSearch(baseMovieList, RANGE_START_YEAR, RANGE_END_YEAR),
                    () -> FileOperations.rangeSearch(RANGE_START_YEAR, RANGE_END_YEAR));

            runUpdateTest(baseMovieList);

            InMemoryOperations.clearResultHolders();
            FileOperations.clearResultHolders();
            System.out.printf("%n✅ 所有性能测试执行完成，资源已清理%n");

        } catch (SQLException | IOException | InterruptedException e) {
            System.err.printf("❌ 测试流程异常终止：%s%n", e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 通用查询场景测试方法（根据测试模式执行对应操作）
     * @param testScene 测试场景名称（用于控制台输出标识）
     * @param dbOp 数据库操作封装（返回耗时：毫秒）
     * @param memOp 内存操作封装（返回耗时：微秒）
     * @param fileOp 文件操作封装（返回耗时：微秒）
     * @throws SQLException 数据库操作异常
     * @throws IOException 文件操作异常
     * @throws InterruptedException Docker操作线程中断异常
     */
    private static void runQueryTest(String testScene,
                                     DbOperation dbOp,
                                     MemoryOperation memOp,
                                     FileOperation fileOp)
            throws SQLException, IOException, InterruptedException {
        System.out.printf("%n=== 📊 %s 性能测试（模式：%s，共%d轮） ===%n",
                testScene, selectedMode, TEST_RUNS);

        if (!DOCKER.isContainerRunning()) {
            DOCKER.startContainer();
        }

        for (int i = 0; i < TEST_RUNS; i++) {
            if (selectedMode == TestMode.DB_VS_MEMORY || selectedMode == TestMode.DB_VS_FILE || (selectedMode == TestMode.SINGLE && selectedSingleType == SingleTestType.DB)) {
                DOCKER.restartContainer();
                DOCKER.discardAll(DEFAULT_DB_USER, DEFAULT_DB_NAME);
            }

            switch (selectedMode) {
                case DB_VS_MEMORY:
                    runDbVsMemory(i, dbOp, memOp);
                    break;
                case DB_VS_FILE:
                    runDbVsFile(i, dbOp, fileOp);
                    break;
                case SINGLE:
                    runSingleType(i, dbOp, memOp, fileOp);
                    break;
            }
        }
    }

    /**
     * 更新场景测试方法（根据测试模式执行对应操作）
     * @param baseMovieList 内存操作的基准数据列表（用于重置内存数据）
     * @throws SQLException 数据库操作异常
     * @throws IOException 文件操作异常
     * @throws InterruptedException Docker操作线程中断异常
     */
    private static void runUpdateTest(List<Movie> baseMovieList)
            throws SQLException, IOException, InterruptedException {
        System.out.printf("%n=== 📊 标题更新操作性能测试（模式：%s，共%d轮） ===%n",
                selectedMode, TEST_RUNS);

        for (int i = 0; i < TEST_RUNS; i++) {
            if (selectedMode == TestMode.DB_VS_MEMORY || selectedMode == TestMode.DB_VS_FILE || (selectedMode == TestMode.SINGLE && selectedSingleType == SingleTestType.DB)) {
                DOCKER.restartContainer();
                DOCKER.discardAll(DEFAULT_DB_USER, DEFAULT_DB_NAME);
            }
            List<Movie> testMovieCopy = new ArrayList<>();
            if (selectedMode == TestMode.DB_VS_MEMORY || (selectedMode == TestMode.SINGLE && selectedSingleType == SingleTestType.MEMORY)) {
                testMovieCopy = createResetMovieCopy(baseMovieList);
            }

            switch (selectedMode) {
                case DB_VS_MEMORY:
                    double dbUpdateMs = DatabaseOperations.updateTitles(DEFAULT_DB_TYPE);
                    long memUpdateUs = InMemoryOperations.updateTitles(testMovieCopy);
                    System.out.printf("第%d轮：数据库=%.3f ms | 内存=%.3f ms%n",
                            i + 1, dbUpdateMs, memUpdateUs / 1000.0);
                    break;
                case DB_VS_FILE:
                    double dbUpdateMs2 = DatabaseOperations.updateTitles(DEFAULT_DB_TYPE);
                    long fileUpdateUs = FileOperations.updateTitles();
                    System.out.printf("第%d轮：数据库=%.3f ms | 文件=%.3f ms%n",
                            i + 1, dbUpdateMs2, fileUpdateUs / 1000.0);
                    break;
                case SINGLE:
                    runSingleTypeUpdate(i, testMovieCopy);
                    break;
            }
        }
    }

    /**
     * 执行「数据库vs内存」查询对比
     * @param round 当前测试轮次
     * @param dbOp 数据库操作
     * @param memOp 内存操作
     * @throws SQLException 数据库异常
     */
    private static void runDbVsMemory(int round, DbOperation dbOp, MemoryOperation memOp) throws SQLException {
        double dbCostMs = dbOp.execute();
        long memCostUs = memOp.execute();
        System.out.printf("第%d轮：数据库=%.3f ms | 内存=%.3f ms%n",
                round + 1, dbCostMs, memCostUs / 1000.0);
    }

    /**
     * 执行「数据库vs文件」查询对比
     * @param round 当前测试轮次
     * @param dbOp 数据库操作
     * @param fileOp 文件操作
     * @throws SQLException 数据库异常
     * @throws IOException 文件异常
     */
    private static void runDbVsFile(int round, DbOperation dbOp, FileOperation fileOp) throws SQLException, IOException {
        double dbCostMs = dbOp.execute();
        long fileCostUs = fileOp.execute();
        System.out.printf("第%d轮：数据库=%.3f ms | 文件=%.3f ms%n",
                round + 1, dbCostMs, fileCostUs / 1000.0);
    }

    /**
     * 执行单类型查询测试（仅数据库/仅内存/仅文件）
     * @param round 当前测试轮次
     * @param dbOp 数据库操作
     * @param memOp 内存操作
     * @param fileOp 文件操作
     * @throws SQLException 数据库异常
     * @throws IOException 文件异常
     */
    private static void runSingleType(int round, DbOperation dbOp, MemoryOperation memOp, FileOperation fileOp) throws SQLException, IOException {
        switch (selectedSingleType) {
            case DB:
                double dbCostMs = dbOp.execute();
                System.out.printf("第%d轮（仅数据库）：耗时=%.3f ms%n", round + 1, dbCostMs);
                break;
            case MEMORY:
                long memCostUs = memOp.execute();
                System.out.printf("第%d轮（仅内存）：耗时=%.3f ms%n", round + 1, memCostUs / 1000.0);
                break;
            case FILE:
                long fileCostUs = fileOp.execute();
                System.out.printf("第%d轮（仅文件）：耗时=%.3f ms%n", round + 1, fileCostUs / 1000.0);
                break;
        }
    }

    /**
     * 执行单类型更新测试（仅数据库/仅内存/仅文件）
     * @param round 当前测试轮次
     * @param testMovieCopy 重置后的内存数据列表
     * @throws SQLException 数据库异常
     * @throws IOException 文件异常
     */
    private static void runSingleTypeUpdate(int round, List<Movie> testMovieCopy) throws SQLException, IOException {
        switch (selectedSingleType) {
            case DB:
                double dbCostMs = DatabaseOperations.updateTitles(DEFAULT_DB_TYPE);
                System.out.printf("第%d轮（仅数据库）：耗时=%.3f ms%n", round + 1, dbCostMs);
                break;
            case MEMORY:
                long memCostUs = InMemoryOperations.updateTitles(testMovieCopy);
                System.out.printf("第%d轮（仅内存）：耗时=%.3f ms%n", round + 1, memCostUs / 1000.0);
                break;
            case FILE:
                long fileCostUs = FileOperations.updateTitles();
                System.out.printf("第%d轮（仅文件）：耗时=%.3f ms%n", round + 1, fileCostUs / 1000.0);
                break;
        }
    }

    /**
     * 创建电影数据副本并重置标题（用于内存更新测试的初始状态一致性）
     * @param baseList 原始基准数据列表
     * @return 重置后的测试用数据副本
     */
    private static List<Movie> createResetMovieCopy(List<Movie> baseList) {
        List<Movie> copyList = new ArrayList<>(baseList.size());
        for (Movie baseMovie : baseList) {
            String resetTitle = baseMovie.getTitle().replace("ttoo", "to");
            Movie copyMovie = new Movie(
                    baseMovie.getMovieId(),
                    resetTitle,
                    baseMovie.getCountry(),
                    baseMovie.getYearReleased(),
                    baseMovie.getRuntime()
            );
            copyList.add(copyMovie);
        }
        return copyList;
    }

    /**
     * 函数式接口：封装数据库操作（查询/更新）
     */
    @FunctionalInterface
    private interface DbOperation {
        double execute() throws SQLException;
    }

    /**
     * 函数式接口：封装内存操作（查询/更新）
     */
    @FunctionalInterface
    private interface MemoryOperation {
        long execute();
    }

    /**
     * 函数式接口：封装文件操作（查询/更新）
     */
    @FunctionalInterface
    private interface FileOperation {
        long execute() throws IOException;
    }
}