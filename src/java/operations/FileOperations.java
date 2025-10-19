package operations;

import model.Movie;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 文件型电影数据操作类
 *
 * <p>该类通过逐行读取文件实现电影数据操作，不加载全量数据到内存，仅在查询时遍历文件，
 * 核心优势是极低的内存占用，适合超大规模数据集场景。设计上与{@link InMemoryOperations}保持接口对齐，
 * 同时保留防JVM即时编译（JIT）优化的机制。</p>
 *
 * <p>文件格式要求：每行对应一条电影记录，格式为「id,'标题','国家',年份,时长」，
 * 标题中若含单引号需转义为两个单引号（如「'Mirror''s War'」对应实际标题「'Mirror's War'」）。</p>
 */
public class FileOperations {
    /** 电影标题最大长度限制（更新标题时遵守） */
    private static final int TITLE_MAX_LENGTH = 100;
    /** 更新操作中需替换的原字符串 */
    private static final String UPDATE_PATTERN_FROM = "to";
    /** 更新操作中替换后的目标字符串 */
    private static final String UPDATE_PATTERN_TO = "ttoo";
    /** 电影数据文件路径 */
    protected static String MOVIE_FILE_PATH = "movies_100000.txt";

    /** 多结果查询的结果持有者（volatile修饰，防JVM优化） */
    private static volatile List<Movie> resultHolder = new ArrayList<>();
    /** 单结果查询的结果持有者（volatile修饰，防JVM优化） */
    private static volatile Movie singleResultHolder;

    /**
     * 设置电影数据文件路径（用于动态配置文件位置）
     * @param movieFilePath 目标文件的绝对路径或相对路径
     */
    public static void setMovieFilePath(String movieFilePath) {
        MOVIE_FILE_PATH = movieFilePath;
    }

    /**
     * 模糊查询 - 逐行读取文件，搜索标题包含指定关键词的电影
     * @param keyword 搜索关键词（区分大小写，匹配逻辑为「标题包含关键词」）
     * @return 操作执行时间（单位：微秒），仅统计文件遍历与查询逻辑耗时
     * @throws IOException  文件读取失败（如文件不存在、权限不足）
     */
    public static long fuzzySearch(String keyword) throws IOException {
        List<Movie> result = new ArrayList<>();
        long startTime = System.nanoTime();

        // 逐行读取文件，解析并匹配条件
        try (BufferedReader reader = new BufferedReader(new FileReader(MOVIE_FILE_PATH))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Movie movie = parseMovieLine(line);
                if (movie != null && movie.getTitle().contains(keyword)) {
                    result.add(movie);
                }
            }
        }

        long endTime = System.nanoTime();
        // 防优化：写入volatile变量
        resultHolder.clear();
        resultHolder.addAll(result);
        System.out.println("防优化措施（文件模糊查询）：" + resultHolder.size());

        return TimeUnit.NANOSECONDS.toMicros(endTime - startTime);
    }

    /**
     * 精确查询 - 逐行读取文件，根据电影ID查询单个电影（找到后终止遍历）
     * @param movieId 目标电影ID（精确匹配）
     * @return 操作执行时间（单位：微秒）
     * @throws IOException  文件读取失败
     */
    public static long exactSearch(int movieId) throws IOException {
        Movie result = null;
        long startTime = System.nanoTime();

        try (BufferedReader reader = new BufferedReader(new FileReader(MOVIE_FILE_PATH))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Movie movie = parseMovieLine(line);
                if (movie != null && movie.getMovieId() == movieId) {
                    result = movie;
                    break; // 找到目标，终止遍历
                }
            }
        }

        long endTime = System.nanoTime();
        // 防优化：写入volatile变量
        singleResultHolder = result;
        System.out.println("防优化措施（文件精确查询）：" + (singleResultHolder != null ? singleResultHolder.getTitle() : "无结果"));

        return TimeUnit.NANOSECONDS.toMicros(endTime - startTime);
    }

    /**
     * 范围查询 - 逐行读取文件，搜索指定年份范围内的电影
     * @param startYear 年份范围起始值（包含）
     * @param endYear 年份范围结束值（包含）
     * @return 操作执行时间（单位：微秒）
     * @throws IOException  文件读取失败
     */
    public static long rangeSearch(int startYear, int endYear) throws IOException {
        List<Movie> result = new ArrayList<>();
        long startTime = System.nanoTime();

        try (BufferedReader reader = new BufferedReader(new FileReader(MOVIE_FILE_PATH))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Movie movie = parseMovieLine(line);
                if (movie != null) {
                    int releaseYear = movie.getYearReleased();
                    if (releaseYear >= startYear && releaseYear <= endYear) {
                        result.add(movie);
                    }
                }
            }
        }

        long endTime = System.nanoTime();
        // 防优化：写入volatile变量
        resultHolder.clear();
        resultHolder.addAll(result);

        return TimeUnit.NANOSECONDS.toMicros(endTime - startTime);
    }

    /**
     * 更新操作 - 逐行修改文件中电影标题的指定字符串（需读写文件）
     * @return 操作执行时间（单位：微秒）
     * @throws IOException  文件读写失败（如文件不可写、磁盘空间不足）
     */
    public static long updateTitles() throws IOException {
        long startTime = System.nanoTime();
        File originalFile = new File(MOVIE_FILE_PATH);
        File tempFile = new File("temp_movies.txt"); // 临时文件存储更新后内容

        // 读取原文件，修改后写入临时文件
        try (BufferedReader reader = new BufferedReader(new FileReader(originalFile));
             BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {

            String line;
            while ((line = reader.readLine()) != null) {
                Movie movie = parseMovieLine(line);
                if (movie == null) {
                    writer.write(line); // 解析失败，保留原行
                    writer.newLine();
                    continue;
                }

                // 执行标题更新逻辑
                String originalTitle = movie.getTitle();
                String newTitle = originalTitle.replace(UPDATE_PATTERN_FROM, UPDATE_PATTERN_TO);
                if (newTitle.length() > TITLE_MAX_LENGTH) {
                    newTitle = originalTitle; // 超过长度限制，不更新
                }

                // 重构更新后的行（恢复单引号转义格式）
                String escapedTitle = newTitle.replace("'", "''");
                String updatedLine = String.format("%d,'%s','%s',%d,%d",
                        movie.getMovieId(),
                        escapedTitle,
                        movie.getCountry(),
                        movie.getYearReleased(),
                        movie.getRuntime());

                writer.write(updatedLine);
                writer.newLine();
            }
        }

        // 替换原文件（删除原文件，重命名临时文件）
        if (!originalFile.delete()) {
            throw new IOException("删除原文件失败，更新终止");
        }
        if (!tempFile.renameTo(originalFile)) {
            throw new IOException("临时文件重命名失败，更新终止");
        }

        long endTime = System.nanoTime();
        // 防优化：读取最后一行验证更新
        try (BufferedReader reader = new BufferedReader(new FileReader(originalFile))) {
            String lastLine = null;
            String line;
            while ((line = reader.readLine()) != null) {
                lastLine = line;
            }
            System.out.println("防优化措施（文件更新）：" + lastLine);
        }

        return TimeUnit.NANOSECONDS.toMicros(endTime - startTime);
    }

    /**
     * 解析文件行为Movie对象（处理单引号转义）
     * @param line 文件中的一行数据（格式：id,'标题','国家',年份,时长）
     * @return 解析后的Movie对象，解析失败返回null
     */
    private static Movie parseMovieLine(String line) {
        try {
            // 按逗号分割（忽略标题内的逗号，基于单引号位置判断）
            String[] parts = line.split(",(?=(?:[^']*'[^']*')*[^']*$)");
            if (parts.length != 5) {
                return null; // 字段数量不匹配，解析失败
            }

            // 解析每个字段，处理单引号转义
            int movieId = Integer.parseInt(parts[0].trim());
            // 标题：去掉前后单引号，将''替换为'
            String title = parts[1].trim().replaceAll("^'|'$", "").replace("''", "'");
            // 国家：去掉前后单引号
            String country = parts[2].trim().replaceAll("^'|'$", "");
            int yearReleased = Integer.parseInt(parts[3].trim());
            Integer runtime = Integer.parseInt(parts[4].trim());

            return new Movie(movieId, title, country, yearReleased, runtime);
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            return null; // 数字格式错误或数组越界，解析失败
        }
    }

    /**
     * 清空结果持有者（释放内存，非业务方法）
     */
    public static void clearResultHolders() {
        if (resultHolder != null) {
            resultHolder.clear();
        }
        singleResultHolder = null;
    }
}