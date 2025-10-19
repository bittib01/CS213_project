package operations;

import model.Movie;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 内存中电影数据操作类
 *
 * <p>该类提供基于内存电影列表的核心操作，包括模糊查询、精确查询、范围查询和标题更新功能，
 * 核心设计目标是确保操作执行时间统计的准确性，因此内置了防 JVM 即时编译（JIT）优化的机制。</p>
 *
 * <p>防优化原理：通过 volatile 变量存储操作结果、触发实际数据读取等方式，避免 JVM 因“结果未被使用”
 * 或 “循环无副作用” 而优化掉核心业务逻辑，导致执行时间统计失真。</p>
 */
public class InMemoryOperations {
    /** 电影标题最大长度限制（更新标题时遵守） */
    private static final int TITLE_MAX_LENGTH = 100;
    /** 更新操作中需替换的原字符串（如 “to” ） */
    private static final String UPDATE_PATTERN_FROM = "to";
    /** 更新操作中替换后的目标字符串（如 “ttoo” ） */
    private static final String UPDATE_PATTERN_TO = "ttoo";

    /**
     * 多结果查询的结果持有者（ volatile 修饰）
     *
     * <p>用于存储列表型查询结果，volatile关键字确保变量写入操作不被编译器/CPU重排序，
     * 且强制内存可见性，防止 JVM 优化掉“仅赋值未使用”的查询循环。</p>
     */
    private static volatile List<Movie> resultHolder = new ArrayList<>();

    /**
     * 单结果查询的结果持有者（ volatile 修饰）
     *
     * <p>功能同{@link #resultHolder}，适配单个电影对象的查询场景（如精确查询），避免JVM优化。</p>
     */
    private static volatile Movie singleResultHolder;

    /**
     * 模糊查询 - 搜索标题包含指定关键词的电影
     *
     * <p>查询结果会存入{@link #resultHolder}，此操作仅为防优化</p>
     *
     * @param movies 数据源，待查询的内存电影列表
     * @param keyword 搜索关键词，匹配逻辑为 “ 电影标题包含该关键词 ”（区分大小写）
     * @return 操作执行时间，单位为微秒（ 1 微秒 = 1000 纳秒），仅统计查询循环耗时
     */
    public static long fuzzySearch(List<Movie> movies, String keyword) {
        List<Movie> result = new ArrayList<>();

        long startTime = System.nanoTime();

        for (Movie movie : movies) {
            if (movie.getTitle().contains(keyword)) {
                result.add(movie);
            }
        }

        long endTime = System.nanoTime();

        resultHolder.clear();
        resultHolder.addAll(result);

        System.out.println("防优化措施，请忽略：" + resultHolder.size());

        return TimeUnit.NANOSECONDS.toMicros(endTime - startTime);
    }

    /**
     * 精确查询 - 根据电影 ID 查询单个电影
     *
     * <p>查询结果会存入{@link #singleResultHolder}，此操作仅为防优化</p>
     *
     * @param movies 数据源，待查询的内存电影列表
     * @param movieId 目标电影ID（精确匹配，int类型）
     * @return 操作执行时间，单位为微秒，找到目标后会提前终止循环
     */
    public static long exactSearch(List<Movie> movies, int movieId) {
        Movie result = null;
        long startTime = System.nanoTime();

        // 核心查询逻辑：遍历列表匹配电影ID，找到后立即终止循环
        for (Movie movie : movies) {
            if (movie.getMovieId() == movieId) {
                result = movie;
                break;
            }
        }

        long endTime = System.nanoTime();

        // 防优化：将单个结果写入volatile变量，避免JVM优化掉循环
        singleResultHolder = result;

        System.out.println("防优化措施，请忽略：" + singleResultHolder.getTitle());

        return TimeUnit.NANOSECONDS.toMicros(endTime - startTime);
    }

    /**
     * 范围查询 - 搜索指定年份范围内的电影
     *
     * <p>1. 查询结果存入{@link #resultHolder}用于防优化；</p>
     * <p>2. 若 {@code startYear} > {@code endYear} ，返回空结果但仍统计耗时</p>
     *
     * @param movies 数据源，待查询的内存电影列表
     * @param startYear 年份范围起始值（包含该年份）
     * @param endYear 年份范围结束值（包含该年份）
     * @return 操作执行时间，单位为微秒
     */
    public static long rangeSearch(List<Movie> movies, int startYear, int endYear) {
        List<Movie> result = new ArrayList<>();
        long startTime = System.nanoTime();

        for (Movie movie : movies) {
            int releaseYear = movie.getYearReleased();
            if (releaseYear >= startYear && releaseYear <= endYear) {
                result.add(movie);
            }
        }

        long endTime = System.nanoTime();

        resultHolder.clear();
        resultHolder.addAll(result);

        return TimeUnit.NANOSECONDS.toMicros(endTime - startTime);
    }

    /**
     * 更新操作 - 批量替换电影标题中的指定字符串（含长度校验）
     *
     * <p>过读取最后一个元素的标题并输出，确保更新循环不被 JVM 判定为 “无副作用” 而优化</p>
     *
     * @param movies 待更新的内存电影列表（直接修改原列表元素，无返回新列表）
     * @return 操作执行时间，单位为微秒，统计所有元素的遍历和符合条件的更新耗时
     */
    public static long updateTitles(List<Movie> movies) {
        long startTime = System.nanoTime();

        for (Movie movie : movies) {
            String originalTitle = movie.getTitle();
            String newTitle = originalTitle.replace(UPDATE_PATTERN_FROM, UPDATE_PATTERN_TO);

            if (newTitle.length() <= TITLE_MAX_LENGTH) {
                movie.setTitle(newTitle);
            }
        }

        long endTime = System.nanoTime();

        if (!movies.isEmpty()) {
            System.out.println("防优化措施，请忽略：" + movies.get(movies.size() - 1).getTitle());
        }

        return TimeUnit.NANOSECONDS.toMicros(endTime - startTime);
    }

    /**
     * 清空结果持有者（仅用于测试或内存释放，非业务方法）
     *
     * <p>因 {@link #resultHolder} 和 {@link #singleResultHolder} 为静态变量，
     * 长时间运行可能占用内存，可通过此方法手动清空。</p>
     */
    public static void clearResultHolders() {
        if (resultHolder != null) {
            resultHolder.clear();
        }
        singleResultHolder = null;
    }
}