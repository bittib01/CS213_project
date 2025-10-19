import pandas as pd
import matplotlib.pyplot as plt
import re
import numpy as np

# --------------------------
# 1. 配置与初始化
# --------------------------
csv_path = "results_runs.csv"
plot_save_path = "db_performance_comparison.png"
plt.rcParams['font.sans-serif'] = ['WenQuanYi Zen Hei', 'SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False


# --------------------------
# 2. 数据加载与清洗
# --------------------------
def load_and_clean_data(csv_path):
    df = pd.read_csv(csv_path, encoding='utf8')
    # 清洗无效数据
    df_clean = df.dropna(subset=['TPS', 'Latency_Avg_ms'], how='any')
    # 转换数值类型
    numeric_cols = ['Clients', 'Threads', 'Run', 'Latency_Avg_ms', 'Init_Connection_Time_ms', 'TPS']
    for col in numeric_cols:
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    df_clean = df_clean.dropna(subset=numeric_cols)

    print(f"✅ 数据加载完成：原始数据{len(df)}行，清洗后有效数据{len(df_clean)}行")
    print(f"📊 包含数据库类型：{df_clean['DB'].unique()}")
    print(f"📊 包含客户端数：{sorted(df_clean['Clients'].unique())}\n")
    return df_clean


df = load_and_clean_data(csv_path)


# --------------------------
# 3. 核心性能指标统计
# --------------------------
def calculate_perf_stats(df):
    stats = df.groupby(['DB', 'Clients']).agg(
        TPS_均值=('TPS', 'mean'),
        TPS_中位数=('TPS', 'median'),
        TPS_标准差=('TPS', 'std'),
        延迟_均值_ms=('Latency_Avg_ms', 'mean'),
        延迟_中位数_ms=('Latency_Avg_ms', 'median'),
        延迟_标准差_ms=('Latency_Avg_ms', 'std'),
        测试次数=('Run', 'count')
    ).round(3)

    stats_reset = stats.reset_index()
    print("=" * 80)
    print("核心性能指标统计（按数据库+客户端数分组）")
    print("=" * 80)
    print(stats_reset.to_string(index=False))
    stats_reset.to_excel("db_perf_stats.xlsx", index=False, engine='openpyxl')
    print(f"\n✅ 统计结果已保存至：db_perf_stats.xlsx")
    return stats_reset


perf_stats = calculate_perf_stats(df)


# --------------------------
# 4. 性能对比可视化
# --------------------------
def plot_performance_comparison(perf_stats):
    postgres_data = perf_stats[perf_stats['DB'] == 'postgresql']
    opengauss_data = perf_stats[perf_stats['DB'] == 'opengauss']
    clients = sorted(perf_stats['Clients'].unique())

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

    # TPS对比
    ax1.plot(postgres_data['Clients'], postgres_data['TPS_均值'],
             marker='o', linewidth=2, color='#2E86AB', label='PostgreSQL（均值）')
    ax1.plot(opengauss_data['Clients'], opengauss_data['TPS_均值'],
             marker='s', linewidth=2, color='#A23B72', label='openGauss（均值）')
    ax1.fill_between(
        postgres_data['Clients'],
        postgres_data['TPS_均值'] - postgres_data['TPS_标准差'],
        postgres_data['TPS_均值'] + postgres_data['TPS_标准差'],
        alpha=0.2, color='#2E86AB'
    )
    ax1.fill_between(
        opengauss_data['Clients'],
        opengauss_data['TPS_均值'] - opengauss_data['TPS_标准差'],
        opengauss_data['TPS_均值'] + opengauss_data['TPS_标准差'],
        alpha=0.2, color='#A23B72'
    )
    ax1.set_ylabel('TPS（每秒事务数）', fontsize=11)
    ax1.set_title('PostgreSQL vs openGauss 性能对比（TPS）', fontsize=13, fontweight='bold')
    ax1.legend(fontsize=10)
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(bottom=0)

    ax2.plot(postgres_data['Clients'], postgres_data['延迟_均值_ms'],
             marker='o', linewidth=2, color='#2E86AB', label='PostgreSQL（均值）')
    ax2.plot(opengauss_data['Clients'], opengauss_data['延迟_均值_ms'],
             marker='s', linewidth=2, color='#A23B72', label='openGauss（均值）')
    ax2.fill_between(
        postgres_data['Clients'],
        postgres_data['延迟_均值_ms'] - postgres_data['延迟_标准差_ms'],
        postgres_data['延迟_均值_ms'] + postgres_data['延迟_标准差_ms'],
        alpha=0.2, color='#2E86AB'
    )
    ax2.fill_between(
        opengauss_data['Clients'],
        opengauss_data['延迟_均值_ms'] - opengauss_data['延迟_标准差_ms'],
        opengauss_data['延迟_均值_ms'] + opengauss_data['延迟_标准差_ms'],
        alpha=0.2, color='#A23B72'
    )
    ax2.set_xlabel('客户端数', fontsize=11)
    ax2.set_ylabel('平均延迟（ms）', fontsize=11)
    ax2.set_title('PostgreSQL vs openGauss 性能对比（延迟）', fontsize=13, fontweight='bold')
    ax2.legend(fontsize=10)
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim(bottom=0)
    ax2.set_xticks(clients)

    plt.tight_layout()
    plt.savefig(plot_save_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"\n✅ 性能对比图已保存至：{plot_save_path}")


plot_performance_comparison(perf_stats)


# --------------------------
# 5. Docker状态解析
# --------------------------
def parse_docker_stats(df):
    def extract_cpu(stats_str):
        if pd.isna(stats_str):
            return np.nan
        cpu_match = re.search(r'CPUPerc: (\d+\.?\d*)%', str(stats_str))
        return float(cpu_match.group(1)) if cpu_match else np.nan

    def extract_mem_usage(stats_str):
        if pd.isna(stats_str):
            return np.nan
        mem_match = re.search(r'MemUsage: (\d+\.?\d*)\w+ /', str(stats_str))
        return float(mem_match.group(1)) if mem_match else np.nan

    df['Docker_CPU_Before'] = df['DockerStats_Before'].apply(extract_cpu)
    df['Docker_Mem_Before_MB'] = df['DockerStats_Before'].apply(extract_mem_usage)

    df['Docker_CPU_After'] = df['DockerStats_After'].apply(extract_cpu)
    df['Docker_Mem_After_MB'] = df['DockerStats_After'].apply(extract_mem_usage)

    df['CPU_差值_%'] = df['Docker_CPU_After'] - df['Docker_CPU_Before']
    df['内存_差值_MB'] = df['Docker_Mem_After_MB'] - df['Docker_Mem_Before_MB']

    docker_stats = df.groupby(['DB', 'Clients']).agg(
        测试前_CPU使用率=('Docker_CPU_Before', 'mean'),
        测试后_CPU使用率=('Docker_CPU_After', 'mean'),
        CPU_差值=('CPU_差值_%', 'mean'),
        测试前_内存_MB=('Docker_Mem_Before_MB', 'mean'),
        测试后_内存_MB=('Docker_Mem_After_MB', 'mean'),
        内存_差值_MB=('内存_差值_MB', 'mean')
    ).round(2)

    print("\n" + "=" * 100)
    print("📊 Docker资源使用统计（含测试后数据及差值：测试后 - 测试前）")
    print("=" * 100)
    print(docker_stats.reset_index().to_string(index=False))

    docker_stats.reset_index().to_excel("docker_resource_stats.xlsx", index=False, engine='openpyxl')
    print(f"\n✅ Docker资源统计（含差值）已保存至：docker_resource_stats.xlsx")

    return df, docker_stats


df_with_docker, docker_stats = parse_docker_stats(df)