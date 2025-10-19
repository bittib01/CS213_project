package model;

/**
 * 电影数据模型类
 *
 * <p>封装电影的核心属性，包括电影ID、标题、国家/地区编码、发行年份和时长，
 * 提供属性的getter和setter方法，确保数据的封装性和安全性。</p>
 */
public class Movie {
    /** 电影唯一标识ID */
    private int movieId;
    /** 电影标题（支持中英文，含单引号转义） */
    private String title;
    /** 国家/地区二字母编码（如CN、US） */
    private String country;
    /** 发行年份 */
    private int yearReleased;
    /** 电影时长（分钟），允许为null（Integer类型） */
    private Integer runtime;

    /**
     * 电影对象构造方法
     *
     * @param movieId 电影唯一ID（非负整数）
     * @param title 电影标题（长度不超过100字符）
     * @param country 国家/地区二字母编码（非空）
     * @param yearReleased 发行年份（通常在1900-2025之间）
     * @param runtime 电影时长（分钟，非负整数，允许为null）
     */
    public Movie(int movieId, String title, String country, int yearReleased, Integer runtime) {
        this.movieId = movieId;
        this.title = title;
        this.country = country;
        this.yearReleased = yearReleased;
        this.runtime = runtime;
    }

    /**
     * 获取电影ID
     * @return 电影唯一标识ID
     */
    public int getMovieId() {
        return movieId;
    }

    /**
     * 设置电影ID
     * @param movieId 新的电影ID（非负整数）
     */
    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    /**
     * 获取电影标题
     * @return 电影标题（已还原单引号转义，即''→'）
     */
    public String getTitle() {
        return title;
    }

    /**
     * 设置电影标题
     * @param title 新的电影标题（长度不超过100字符，如需存储需处理单引号转义）
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * 获取国家/地区编码
     * @return 二字母国家/地区编码（如CN、US）
     */
    public String getCountry() {
        return country;
    }

    /**
     * 设置国家/地区编码
     * @param country 新的二字母国家/地区编码（非空）
     */
    public void setCountry(String country) {
        this.country = country;
    }

    /**
     * 获取发行年份
     * @return 电影发行年份
     */
    public int getYearReleased() {
        return yearReleased;
    }

    /**
     * 设置发行年份
     * @param yearReleased 新的发行年份（通常在1900-2025之间）
     */
    public void setYearReleased(int yearReleased) {
        this.yearReleased = yearReleased;
    }

    /**
     * 获取电影时长
     * @return 电影时长（分钟），可能为null
     */
    public Integer getRuntime() {
        return runtime;
    }

    /**
     * 设置电影时长
     * @param runtime 新的电影时长（分钟，非负整数，允许为null）
     */
    public void setRuntime(Integer runtime) {
        this.runtime = runtime;
    }
}