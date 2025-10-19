package util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

/**
 * Docker容器操作工具类
 *
 * <p>该类提供了一系列操作 Docker 容器的方法，主要用于管理 PostgreSQL 等数据库容器，
 * 包括容器的启动、停止、重启、状态检查、日志获取及执行容器内命令等功能。</p>
 *
 * <p>特性说明：</p>
 * <ul>
 *   <li>支持命令执行结果校验与错误处理</li>
 *   <li>同时捕获命令的标准输出与错误输出，避免进程阻塞</li>
 *   <li>提供操作超时控制，增强程序健壮性</li>
 *   <li>可通过构造函数自定义容器名称，适应不同环境</li>
 * </ul>
 */
public class Docker {
    /** 默认容器名称（PostgreSQL数据库容器） */
    private static final String DEFAULT_CONTAINER_NAME = "postgresql";

    /** 命令执行默认超时时间（秒） */
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;

    /** 容器操作后默认等待时间（毫秒），用于容器状态稳定 */
    private static final int DEFAULT_WAIT_MS = 5000;

    /** 当前操作的容器名称 */
    private final String containerName;

    /**
     * 构造方法，使用默认容器名称
     */
    public Docker() {
        this(DEFAULT_CONTAINER_NAME);
    }

    /**
     * 构造方法，自定义容器名称
     *
     * @param containerName 要操作的Docker容器名称
     */
    public Docker(String containerName) {
        this.containerName = containerName;
    }

    /**
     * 重启容器
     *
     * @throws IOException          当命令执行失败或 IO 操作异常时抛出
     * @throws InterruptedException 当线程等待被中断时抛出
     * @throws RuntimeException     当命令执行返回非 0 状态码时抛出
     */
    public void restartContainer() throws IOException, InterruptedException {
        executeDockerCommand("restart " + containerName, true);
        Thread.sleep(DEFAULT_WAIT_MS);
    }

    /**
     * 启动容器（若容器未运行）
     *
     * @throws IOException          当命令执行失败或 IO 操作异常时抛出
     * @throws InterruptedException 当线程等待被中断时抛出
     * @throws RuntimeException     当命令执行返回非 0 状态码时抛出
     */
    public void startContainer() throws IOException, InterruptedException {
        executeDockerCommand("start " + containerName, true);
        Thread.sleep(DEFAULT_WAIT_MS);
    }

    /**
     * 停止容器
     *
     * @throws IOException          当命令执行失败或 IO 操作异常时抛出
     * @throws InterruptedException 当线程等待被中断时抛出
     * @throws RuntimeException     当命令执行返回非 0 状态码时抛出
     */
    public void stopContainer() throws IOException, InterruptedException {
        executeDockerCommand("stop " + containerName, true);
        Thread.sleep(DEFAULT_WAIT_MS);
    }

    /**
     * 检查容器运行状态
     *
     * @return true-容器正在运行，false-容器未运行
     * @throws IOException          当命令执行失败或IO操作异常时抛出
     * @throws InterruptedException 当线程等待被中断时抛出
     */
    public boolean isContainerRunning() throws IOException, InterruptedException {
        CommandResult result = executeDockerCommand("inspect -f '{{.State.Running}}' " + containerName, false);
        // 正常运行时输出为"true"，去除可能的引号和空格
        String output = result.output().trim().replace("'", "");
        return "true".equals(output);
    }

    /**
     * 获取容器最近的日志
     *
     * @param lines 要获取的日志行数
     * @return 容器日志内容
     * @throws IOException          当命令执行失败或 IO 操作异常时抛出
     * @throws InterruptedException 当线程等待被中断时抛出
     * @throws RuntimeException     当命令执行返回非 0 状态码时抛出
     */
    public String getContainerLogs(int lines) throws IOException, InterruptedException {
        CommandResult result = executeDockerCommand("logs --tail " + lines + " " + containerName, true);
        return result.output();
    }

    /**
     * 在容器内执行DISCARD ALL命令，清除数据库缓存
     *
     * @param username 数据库用户名
     * @param dbName   数据库名称
     * @throws IOException          当命令执行失败或 IO 操作异常时抛出
     * @throws InterruptedException 当线程等待被中断时抛出
     * @throws RuntimeException     当命令执行返回非 0 状态码时抛出
     */
    public void discardAll(String username, String dbName) throws IOException, InterruptedException {
        String command = String.format(
                "exec %s psql -U %s -d %s -c \"DISCARD ALL;\"",
                containerName, username, dbName
        );
        executeDockerCommand(command, true);
    }

    /**
     * 执行自定义 Docker 命令
     *
     * @param command 要执行的 Docker 子命令（如 "ps -a" ）
     * @param checkSuccess 是否检查命令执行结果（ true-非 0 状态码抛出异常）
     * @return 命令执行结果（包含输出内容和状态码）
     * @throws IOException          当命令执行失败或 IO 操作异常时抛出
     * @throws InterruptedException 当线程等待被中断时抛出
     * @throws RuntimeException     当 checkSuccess 为 true 且命令返回非 0 状态码时抛出
     */
    public CommandResult executeDockerCommand(String command, boolean checkSuccess) throws IOException, InterruptedException {
        String fullCommand = "docker " + command;
        Process process = Runtime.getRuntime().exec(fullCommand);

        StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream());
        StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream());

        outputGobbler.start();
        errorGobbler.start();

        boolean finished = process.waitFor(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            throw new RuntimeException("命令执行超时: " + fullCommand);
        }

        outputGobbler.join();
        errorGobbler.join();

        int exitCode = process.exitValue();
        String output = outputGobbler.getContent();
        String error = errorGobbler.getContent();

        if (!error.isEmpty()) {
            output += "\n错误输出: " + error;
        }

        if (checkSuccess && exitCode != 0) {
            throw new RuntimeException(
                    String.format("命令执行失败 (状态码: %d): %s%n输出: %s",
                            exitCode, fullCommand, output)
            );
        }

        return new CommandResult(exitCode, output);
    }

    /**
     * 命令执行结果封装类
     */
    public record CommandResult(int exitCode, String output) {
        /**
         * 构造命令执行结果
         *
         * @param exitCode 命令退出状态码
         * @param output   命令输出内容
         */
        public CommandResult {
        }

            /**
             * 获取命令退出状态码
             *
             * @return 状态码（0表示成功）
             */
            @Override
            public int exitCode() {
                return exitCode;
            }

            /**
             * 获取命令输出内容
             *
             * @return 标准输出与错误输出的合并内容
             */
            @Override
            public String output() {
                return output;
            }
        }

    /**
     * 流处理线程类，用于异步读取输入流，避免进程阻塞
     */
    private static class StreamGobbler extends Thread {
        private final InputStream inputStream;
        private final StringBuilder content = new StringBuilder();

        /**
         * 构造流处理器
         *
         * @param inputStream 要处理的输入流（如进程的标准输出或错误输出）
         */
        public StreamGobbler(InputStream inputStream) {
            this.inputStream = inputStream;
        }

        @Override
        public void run() {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(inputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    content.append(line).append("\n");
                }
            } catch (IOException e) {
                content.append("流读取异常: ").append(e.getMessage()).append("\n");
            }
        }

        /**
         * 获取流处理的内容
         *
         * @return 读取到的流内容
         */
        public String getContent() {
            return content.toString().trim();
        }
    }
}
