import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

raw_data = [
    ["Fuzzy Query (title contains 'the')", 1, 7.905, 116.931, 323.780, 6.806],
    [None, 2, 7.091, 26.771, 195.863, 2.913],
    [None, 3, 7.259, 23.784, 148.744, 2.329],
    [None, 4, 6.859, 23.241, 167.387, 1.862],
    [None, 5, 6.831, 23.118, 151.732, 1.898],
    [None, 6, 7.179, 22.664, 149.042, 2.168],
    [None, 7, 7.101, 22.886, 155.299, 2.126],
    [None, 8, 7.416, 21.844, 148.968, 1.998],
    [None, 9, 7.456, 23.576, 147.209, 2.039],
    [None, 10, 7.035, 22.548, 143.558, 2.041],
    [None, 11, 6.676, 28.586, 147.774, 1.933],
    [None, 12, 7.018, 25.956, 146.251, 2.093],
    [None, 13, 7.040, 23.457, 146.573, 1.990],
    [None, 14, 7.068, 22.572, 142.776, 2.277],
    [None, 15, 7.218, 22.183, 142.533, 1.940],
    [None, 16, 6.864, 22.589, 146.098, 2.028],
    [None, 17, 6.995, 24.593, 144.364, 1.979],
    [None, 18, 7.119, 22.336, 145.829, 1.909],
    [None, 19, 7.163, 22.510, 144.688, 1.855],
    [None, 20, 7.133, 22.051, 147.339, 1.968],

    ["Exact Query (movie ID=1000)", 1, 0.314, 0.551, 1.972, 0.375],
    [None, 2, 0.043, 0.067, 1.845, 0.363],
    [None, 3, 0.043, 0.101, 2.084, 0.363],
    [None, 4, 0.044, 0.055, 1.960, 0.352],
    [None, 5, 0.043, 0.052, 1.946, 0.362],
    [None, 6, 0.042, 0.056, 1.958, 0.399],
    [None, 7, 0.043, 0.051, 1.808, 0.392],
    [None, 8, 0.044, 0.054, 2.102, 0.370],
    [None, 9, 0.050, 0.052, 1.826, 0.387],
    [None, 10, 0.045, 0.049, 1.962, 0.112],
    [None, 11, 0.054, 0.054, 1.950, 0.174],
    [None, 12, 0.043, 0.056, 1.851, 0.099],
    [None, 13, 0.043, 0.054, 1.999, 0.086],
    [None, 14, 0.043, 0.058, 1.874, 0.114],
    [None, 15, 0.043, 0.055, 1.971, 0.146],
    [None, 16, 0.068, 0.052, 2.016, 0.123],
    [None, 17, 0.044, 0.051, 1.821, 0.156],
    [None, 18, 0.045, 0.049, 1.988, 0.083],
    [None, 19, 0.046, 0.053, 2.107, 0.038],
    [None, 20, 0.046, 0.051, 1.862, 0.032],

    ["Range Query (year 1990-2000)", 1, 3.960, 29.651, 149.850, 4.509],
    [None, 2, 4.109, 37.478, 148.050, 2.007],
    [None, 3, 3.935, 40.139, 149.343, 1.402],
    [None, 4, 3.920, 30.081, 144.442, 1.078],
    [None, 5, 3.818, 30.736, 143.968, 1.260],
    [None, 6, 4.020, 30.507, 148.398, 0.992],
    [None, 7, 3.986, 29.697, 144.485, 1.187],
    [None, 8, 3.844, 31.058, 145.961, 1.079],
    [None, 9, 4.172, 31.059, 156.620, 1.241],
    [None, 10, 3.766, 30.452, 142.314, 1.018],
    [None, 11, 4.140, 30.180, 142.971, 1.156],
    [None, 12, 5.506, 35.323, 147.478, 1.071],
    [None, 13, 3.949, 30.547, 148.146, 1.092],
    [None, 14, 3.910, 30.533, 146.174, 1.120],
    [None, 15, 3.814, 34.962, 144.610, 1.014],
    [None, 16, 4.572, 29.932, 144.504, 1.113],
    [None, 17, 4.354, 29.732, 146.717, 1.004],
    [None, 18, 3.822, 32.874, 145.337, 1.075],
    [None, 19, 3.787, 30.017, 144.958, 1.022],
    [None, 20, 5.880, 31.055, 143.313, 1.148],

    ["Title Update Operation", 1, 1376.695, 1734.905, 288.134, 8.555],
    [None, 2, 1388.919, 1548.038, 228.934, 3.736],
    [None, 3, 1627.126, 1563.725, 231.256, 2.825],
    [None, 4, 1755.811, 1518.206, 214.343, 2.805],
    [None, 5, 1726.361, 1500.348, 216.389, 2.544],
    [None, 6, 1583.826, 1507.929, 218.564, 2.699],
    [None, 7, 1521.653, 1552.117, 214.249, 2.794],
    [None, 8, 1551.465, 1535.368, 218.795, 2.524],
    [None, 9, 1529.230, 1518.544, 221.097, 2.553],
    [None, 10, 1576.290, 1562.110, 220.216, 2.669],
    [None, 11, 1569.110, 1531.973, 217.984, 2.599],
    [None, 12, 1546.331, 1529.366, 216.747, 2.631],
    [None, 13, 1551.169, 1565.216, 224.888, 2.677],
    [None, 14, 1503.592, 1559.585, 216.144, 2.476],
    [None, 15, 1528.739, 1547.139, 214.039, 2.632],
    [None, 16, 1551.828, 1527.525, 218.886, 2.815],
    [None, 17, 1551.190, 1560.193, 222.252, 2.620],
    [None, 18, 1627.511, 1529.346, 220.767, 2.553],
    [None, 19, 1641.582, 1549.169, 220.455, 2.564],
    [None, 20, 1583.050, 1542.450, 220.604, 2.655]
]

columns = ["test_type", "round_num", "db1_time_ms", "db2_time_ms", "file_time_ms", "memory_time_ms"]
df = pd.DataFrame(raw_data, columns=columns)
df["test_type"] = df["test_type"].fillna(method="ffill")
df["data_size"] = 100000


time_cols = ["db1_time_ms", "db2_time_ms", "file_time_ms", "memory_time_ms"]
for col in time_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

print("=== Raw Data Construction Completed (100000 Records) ===")
print(f"Total Rounds: {len(df)} (4 test types × 20 rounds)")
print(f"Test Type Distribution: {df['test_type'].value_counts().to_dict()}")
print("\n")


df["total_time_avg"] = df[time_cols].mean(axis=1)

def remove_outliers(group):
    """Remove 2 smallest and 2 largest rounds by total_time_avg (keep middle 16 rounds)"""
    if len(group) < 4:
        return group
    sorted_group = group.sort_values("total_time_avg")
    return sorted_group.iloc[2:-2]


df_cleaned = df.groupby("test_type", group_keys=False).apply(remove_outliers)
df_cleaned = df_cleaned.drop(columns=["total_time_avg"])


print("=== Data Cleaning Comparison (100000 Records) ===")
print(f"Raw Data Rounds: {len(df)}")
print(f"Rounds After Outlier Removal: {len(df_cleaned)} (4 test types × 16 rounds)")
for test_type in df["test_type"].unique():
    original_count = len(df[df["test_type"] == test_type])
    cleaned_count = len(df_cleaned[df_cleaned["test_type"] == test_type])
    print(f"  {test_type}: {original_count} rounds → {cleaned_count} rounds")
print("\n")



stats_cleaned = df_cleaned.groupby("test_type")[time_cols].agg(["mean", "std"]).round(3)

stats_cleaned.columns = [
    "db1_mean_ms", "db1_std_ms",
    "db2_mean_ms", "db2_std_ms",
    "file_mean_ms", "file_std_ms",
    "memory_mean_ms", "memory_std_ms"
]

print("=== Performance Statistics (100000 Records After Outlier Removal) ===")
print(stats_cleaned)
print("\n")


plt.rcParams["font.sans-serif"] = ["SimHei", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False
plt.style.use("seaborn-v0_8-whitegrid")


test_type_map = {
    "Fuzzy Query (title contains 'the')": "Fuzzy Query",
    "Exact Query (movie ID=1000)": "Exact Query",
    "Range Query (year 1990-2000)": "Range Query",
    "Title Update Operation": "Title Update"
}
df_plot = df_cleaned.copy()
df_plot["test_type_simplified"] = df_plot["test_type"].map(test_type_map)


df_mean = df_plot.groupby("test_type_simplified")[time_cols].mean().reset_index()

x = np.arange(len(df_mean))
width = 0.2
fig, ax = plt.subplots(figsize=(14, 9))

bars1 = ax.bar(x - 1.5*width, df_mean["db1_time_ms"], width, label="DB1 (DB_VS_FILE)", color="#1f77b4")
bars2 = ax.bar(x - 0.5*width, df_mean["db2_time_ms"], width, label="DB2 (DB_VS_MEMORY)", color="#ff7f0e")
bars3 = ax.bar(x + 0.5*width, df_mean["file_time_ms"], width, label="File Storage", color="#2ca02c")
bars4 = ax.bar(x + 1.5*width, df_mean["memory_time_ms"], width, label="Memory Storage", color="#d62728")

ax.set_title("100000 Records (After Outlier Removal): Storage Time Comparison by Test Type", fontsize=16, pad=20)
ax.set_xlabel("Test Type", fontsize=12)
ax.set_ylabel("Average Time (ms)", fontsize=12)
ax.set_xticks(x)
ax.set_xticklabels(df_mean["test_type_simplified"], rotation=0)
ax.legend(fontsize=10)

def add_labels(bars):
    for bar in bars:
        h = bar.get_height()
        if h > 1:
            ax.text(bar.get_x() + bar.get_width()/2, h, f"{h:.1f}", ha="center", va="bottom", fontsize=9)
        elif h > 0.1:
            ax.text(bar.get_x() + bar.get_width()/2, h, f"{h:.3f}", ha="center", va="bottom", fontsize=8)

add_labels(bars1)
add_labels(bars2)
add_labels(bars3)
add_labels(bars4)

plt.tight_layout()
plt.savefig("100000_records_storage_time_comparison_cleaned.png", dpi=300, bbox_inches="tight")
plt.close()


fig, axes = plt.subplots(2, 2, figsize=(16, 12))
axes = axes.flatten()

for i, (test_type, ax) in enumerate(zip(df_cleaned["test_type"].unique(), axes)):

    df_test = df_cleaned[df_cleaned["test_type"] == test_type][time_cols]

    box_plot = ax.boxplot(
        [df_test[col] for col in time_cols],
        labels=["DB1", "DB2", "File", "Memory"],
        patch_artist=True,
        showfliers=False
    )

    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"]
    for patch, color in zip(box_plot["boxes"], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)

    if test_type == "Title Update Operation":
        ax.set_ylim(0, max(df_test["db1_time_ms"]) * 1.1) 

    ax.set_title(f"{test_type_map[test_type]} (After Outlier Removal)", fontsize=12)
    ax.set_ylabel("Time (ms)", fontsize=10)
    ax.grid(True, alpha=0.3)

# Main title
fig.suptitle("100000 Records: Storage Time Distribution by Test Type (Box Plot)", fontsize=16, y=0.98)
plt.tight_layout()
plt.savefig("100000_records_time_distribution_boxplot_cleaned.png", dpi=300, bbox_inches="tight")
plt.close()


print("=== Core Conclusions (100000 Records After Outlier Removal) ===")

fuzzy_stats = stats_cleaned.loc["Fuzzy Query (title contains 'the')"]
exact_stats = stats_cleaned.loc["Exact Query (movie ID=1000)"]
range_stats = stats_cleaned.loc["Range Query (year 1990-2000)"]
update_stats = stats_cleaned.loc["Title Update Operation"]