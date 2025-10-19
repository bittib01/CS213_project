package util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * 电影数据生成器
 * <p>
 * 该类用于生成 movies 表的 INSERT 语句文件，包含的电影信息包括：
 * 电影ID、标题、国家、发行年份和时长等。
 *
 * <h3>使用方法：</h3>
 * <pre>
 *   java util.GenerateMovies [生成数量] [起始ID] [输出文件]
 * </pre>
 *
 * <h3>参数说明：</h3>
 * <ul>
 *   <li>生成数量：需要生成的电影记录条数</li>
 *   <li>起始ID：生成的第一条记录的 ID 值</li>
 *   <li>输出文件：保存生成的 SQL 语句的文件路径</li>
 * </ul>
 *
 * <h3>默认值：</h3>
 * <ul>
 *   <li>生成数量 = 100000</li>
 *   <li>起始ID = 1</li>
 *   <li>输出文件 = movies_&lt;生成数量&gt;.sql（例如：movies_100000.sql）</li>
 * </ul>
 */
public class GenerateMovies {

    private static final String[] ADJECTIVES = {
            "Red", "红色", "Blue", "蓝色", "Green", "绿色", "Yellow", "黄色", "Black", "黑色",
            "White", "白色", "Purple", "紫色", "Orange", "橙色", "Gray", "灰色", "Pink", "粉色",

            "Happy", "快乐的", "Sad", "悲伤的", "Angry", "愤怒的", "Calm", "平静的", "Brave", "勇敢的",
            "Cowardly", "懦弱的", "Lonely", "孤独的", "Loving", "友爱的", "Fierce", "凶猛的", "Gentle", "温柔的",

            "Dark", "黑暗的", "Bright", "明亮的", "Silent", "寂静的", "Loud", "喧闹的", "Wild", "狂野的",
            "Tame", "温顺的", "Hidden", "隐藏的", "Visible", "可见的", "Lost", "迷失的", "Found", "被找到的",
            "Broken", "破碎的", "Whole", "完整的", "Ancient", "古老的", "Modern", "现代的", "Burning", "燃烧的",
            "Frozen", "冰封的", "Eternal", "永恒的", "Temporary", "暂时的", "Little", "微小的", "Big", "巨大的",
            "Crazy", "疯狂的", "Sane", "理智的", "Quiet", "安静的", "Noisy", "嘈杂的", "Far", "遥远的",
            "Near", "临近的", "Mysterious", "神秘的", "Obvious", "明显的", "Rare", "稀有的", "Common", "普通的",

            "Golden", "金色的", "Silver", "银色的", "Precious", "珍贵的", "Valuable", "有价值的", "Worthless", "无价值的",
            "Strong", "强壮的", "Weak", "虚弱的", "Fast", "快速的", "Slow", "缓慢的", "Sharp", "锋利的",
            "Dull", "迟钝的", "Smooth", "平滑的", "Rough", "粗糙的", "Clean", "干净的", "Dirty", "肮脏的",
            "Perfect", "完美的", "Flawed", "有缺陷的", "Beautiful", "美丽的", "Ugly", "丑陋的", "Noble", "高尚的",
            "Vile", "卑鄙的", "Honest", "诚实的", "Deceitful", "欺诈的"
    };

    private static final String[] NOUNS = {
            "River", "河流", "Mountain", "山脉", "Ocean", "海洋", "Forest", "森林", "Desert", "沙漠",
            "Storm", "风暴", "Snow", "雪", "Rain", "雨", "Wind", "风", "Fire", "火", "Ice", "冰",
            "Sun", "太阳", "Moon", "月亮", "Star", "星星", "Sky", "天空", "Earth", "大地", "Stone", "石头",
            "Flower", "花朵", "Tree", "树", "Leaf", "树叶", "Seed", "种子",

            "City", "城市", "Village", "村庄", "House", "房屋", "Castle", "城堡", "Tower", "塔",
            "Bridge", "桥梁", "Road", "道路", "Path", "小径", "Ocean", "海洋", "Island", "岛屿",
            "Cave", "洞穴", "Valley", "山谷", "Meadow", "草地", "Forest", "森林", "Desert", "沙漠",

            "Dream", "梦想", "Memory", "记忆", "Hope", "希望", "Fear", "恐惧", "Love", "爱",
            "Hate", "恨", "Truth", "真相", "Lie", "谎言", "Freedom", "自由", "Prison", "监狱",
            "Time", "时间", "Death", "死亡", "Life", "生命", "Fate", "命运", "Chance", "机会",
            "Courage", "勇气", "Cowardice", "懦弱", "Wisdom", "智慧", "Folly", "愚蠢", "War", "战争",
            "Peace", "和平", "Justice", "正义", "Injustice", "不公",

            "Child", "孩子", "Man", "男人", "Woman", "女人", "King", "国王", "Queen", "女王",
            "Hero", "英雄", "Villain", "恶棍", "Friend", "朋友", "Enemy", "敌人", "Stranger", "陌生人",
            "Traveler", "旅行者", "Wanderer", "流浪者", "Warrior", "战士", "Scholar", "学者", "Artist", "艺术家",

            "Sword", "剑", "Shield", "盾牌", "Book", "书", "Letter", "信件", "Mirror", "镜子",
            "Clock", "时钟", "Key", "钥匙", "Box", "盒子", "Ship", "船", "Car", "汽车", "Train", "火车",
            "Plane", "飞机", "Song", "歌曲", "Melody", "旋律", "Whisper", "低语", "Secret", "秘密",
            "Treasure", "宝藏", "Gift", "礼物", "Curse", "诅咒", "Blessing", "祝福"
    };

    private static final String[] CONNECTORS = {
            " ", " ", " - ", " - ", ": ", "：", " of ", " 的 ", " & ", " 和 ",
            "'s ", " 的 ", " — ", " —— ", " in ", " 中的 ", " under ", " 在 ",
            " over ", " 之上 ", " beyond ", " 超越 ", " vs ", " 对 ", " with ", " 与 ",
            " within ", " 之内 ", " without ", " 之外 ", " through ", " 穿越 ", " from ", " 来自 ",
            " to ", " 向 ", " for ", " 为了 ", " about ", " 关于 ", " after ", " 之后 ", " before ", " 之前 "
    };

    private static final String[] COUNTRIES = {
            "US","CN","JP","DE","FR","GB","RU","EG","IN","BR","CA","AU","IT","ES","SE","NO","DK",
            "NL","BE","CH","AT","KR","MX","AR","ZA","TR","PL","GR","PT","IE","IL","SA","AE","ID",
            "MY","PH","SG","TH","VN","NG","KE","CO","PE","CL","HU","CZ","RO","BG","HR","SI","SK"
    };

    private final Random rnd = new Random();

    private final Set<String> seenTriples = new HashSet<>();

    /**
     * 程序主入口
     * 解析命令行参数，设置默认值，并调用生成方法
     * @param args 命令行参数，依次为：生成数量、起始ID、输出文件路径
     * @throws Exception 处理过程中可能抛出的异常
     */
    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 100000;
        int startId = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        String outFile = args.length > 2 ? args[2] : "movies_" + count + ".sql";

        GenerateMovies generator = new GenerateMovies();
        generator.generate(count, startId, outFile);
    }

    /**
     * 生成电影数据并写入 SQL 文件
     * @param count 生成的记录数量
     * @param startId 起始ID值
     * @param outFile 输出文件路径
     * @throws IOException 写入文件时可能抛出的IO异常
     */
    public void generate(int count, int startId, String outFile) throws IOException {
        Path path = Paths.get(outFile);
        try (BufferedWriter bw = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            bw.write("-- 生成的记录数: " + count + "\n\n");

            bw.write("""
                    CREATE TABLE movies(movieid       integer not null primary key,
                                        title         varchar(100) not null
                                                     constraint "title length"
                                                       check(length(title)<=100),
                                        country       char(2) not null
                                                     constraint "country length"
                                                       check(length(country)<=2),
                                        year_released int not null
                                                     constraint "year_released numerical"
                                                       check(year_released+0=year_released),
                                        runtime        int
                                                     constraint "runtime numerical"
                                                       check(runtime+0=runtime),
                                        unique(title, country, year_released));
                    """);

            for (int i = 0; i < count; i++) {
                int id = startId + i;
                StringBuilder title = new StringBuilder(generateTitle());
                String country = pickCountry();
                int year = pickYear();
                int runtime = pickRuntime();

                String key = title + "|" + country + "|" + year;
                int attempts = 0;
                while (seenTriples.contains(key)) {
                    String suffix = " " + (rnd.nextInt(9000) + 1000);
                    if (title.length() + suffix.length() > 100) {
                        title = new StringBuilder(title.substring(0, Math.max(0, 100 - suffix.length())));
                    }
                    title.append(suffix);
                    key = title + "|" + country + "|" + year;
                    attempts++;

                    if (attempts > 20) {
                        country = pickCountry();
                        year = pickYear();
                        key = title + "|" + country + "|" + year;
                    }
                }
                seenTriples.add(key);

                String escapedTitle = title.toString().replace("'", "''");
                String sql = String.format("INSERT INTO movies VALUES(%d,'%s','%s',%d,%d);",
                        id, escapedTitle, country, year, runtime);
                bw.write(sql);
                bw.newLine();

                // 每生成10000条记录输出一次进度
                if ((i + 1) % 10000 == 0) {
                    System.out.println("已生成 " + (i + 1) + " / " + count + " 条记录...");
                }
            }
        }

        System.out.println("生成完成。输出文件: " + outFile);
    }

    /**
     * 生成电影标题
     * 标题由2-4个部分组成，通过形容词、名词和连接符组合而成
     * @return 生成的电影标题
     */
    private String generateTitle() {
        int pattern = rnd.nextInt(6);
        String title = switch (pattern) {
            case 0 -> pickWord(ADJECTIVES) + pickConnector() + pickWord(NOUNS);
            case 1 -> pickWord(NOUNS) + pickConnector() + pickWord(NOUNS);
            case 2 -> pickWord(ADJECTIVES) + " " + pickWord(NOUNS) + pickConnector() + pickWord(NOUNS);
            case 3 -> pickWord(NOUNS) + " " + (rnd.nextInt(100) + 1);
            case 4 -> pickWord(ADJECTIVES) + " " + pickWord(NOUNS);
            default -> pickWord(NOUNS) + pickConnector() + pickWord(ADJECTIVES);
        };

        if (rnd.nextDouble() < 0.12) {
            title = title + " - " + pickShortToken();
        }

        if (title.length() > 100) {
            title = title.substring(0, 100);
        }
        return title;
    }

    /**
     * 从指定数组中随机选择一个元素
     * @param arr 字符串数组
     * @return 随机选中的元素
     */
    private String pickWord(String[] arr) {
        return arr[rnd.nextInt(arr.length)];
    }

    /**
     * 随机选择一个连接符
     * @return 选中的连接符
     */
    private String pickConnector() {
        return CONNECTORS[rnd.nextInt(CONNECTORS.length)];
    }

    /**
     * 随机选择一个短标记（用于标题补充）
     * @return 选中的短标记
     */
    private String pickShortToken() {
        String[] tokens = {
                "II", "第二部", "III", "第三部", "IV", "第四部",
                "Revisited", "重访", "Redux", "加长版",
                "Part I", "第一部", "Part II", "第二部",
                "Remix", "混音版", "Orig", "原版",
                "Saga", "传奇", "Vol. 1", "卷一",
                "Vol. 2", "卷二", "Return", "归来",
                "Begins", "起源", "End", "终结",
                "Rise", "崛起", "Fall", "陨落", "Chronicles", "编年史"
        };
        return tokens[rnd.nextInt(tokens.length)];
    }

    /**
     * 随机选择一个国家/地区编码
     * @return 选中的国家/地区编码
     */
    private String pickCountry() {
        return COUNTRIES[rnd.nextInt(COUNTRIES.length)];
    }

    /**
     * 随机选择一个发行年份（1900-2025之间）
     * @return 选中的年份
     */
    private int pickYear() {
        return 1900 + rnd.nextInt(126); // 1900 + 0~125 → 1900~2025
    }

    /**
     * 随机选择一个电影时长（60-240分钟之间）
     * @return 选中的时长（分钟）
     */
    private int pickRuntime() {
        return 60 + rnd.nextInt(181);
    }
}
