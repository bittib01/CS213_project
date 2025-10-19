import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

raw_data = [
    ["Fuzzy Query (title contains 'the')", 1, 5.690, 1.101, 79.024, 1.427],
    [None, 2, 6.306, 1.019, 76.521, 0.884],
    [None, 3, 5.702, 0.973, 22.429, 0.769],
    [None, 4, 5.782, 0.991, 20.483, 0.760],
    [None, 5, 5.695, 1.092, 20.658, 0.749],
    [None, 6, 5.891, 0.974, 21.919, 0.767],
    [None, 7, 6.557, 1.183, 15.682, 0.860],
    [None, 8, 6.371, 0.990, 16.134, 0.314],
    [None, 9, 6.049, 1.069, 16.569, 0.416],
    [None, 10, 6.854, 1.004, 15.328, 0.259],
    [None, 11, 6.535, 1.118, 17.126, 0.299],
    [None, 12, 6.792, 1.076, 16.175, 0.270],
    [None, 13, 6.911, 0.937, 14.504, 0.191],
    [None, 14, 8.099, 1.035, 14.552, 0.196],
    [None, 15, 9.132, 0.988, 14.923, 0.246],
    [None, 16, 7.832, 1.272, 14.994, 0.273],
    [None, 17, 6.829, 1.007, 14.847, 0.175],
    [None, 18, 7.645, 0.999, 14.221, 0.192],
    [None, 19, 9.727, 0.980, 14.973, 0.246],
    [None, 20, 7.392, 1.001, 14.578, 0.226],

    ["Exact Query (movie ID=1000)", 1, 0.066, 0.524, 2.147, 0.136],
    [None, 2, 0.045, 0.048, 1.773, 0.058],
    [None, 3, 0.045, 0.043, 1.853, 0.085],
    [None, 4, 0.045, 0.045, 2.251, 0.056],
    [None, 5, 0.045, 0.043, 2.041, 0.054],
    [None, 6, 0.045, 0.048, 1.790, 0.058],
    [None, 7, 0.050, 0.046, 1.818, 0.057],
    [None, 8, 0.044, 0.046, 2.115, 0.070],
    [None, 9, 0.044, 0.041, 2.032, 0.058],
    [None, 10, 0.043, 0.045, 2.129, 0.079],
    [None, 11, 0.088, 0.043, 1.958, 0.057],
    [None, 12, 0.052, 0.043, 1.975, 0.061],
    [None, 13, 0.044, 0.053, 2.078, 0.071],
    [None, 14, 0.048, 0.047, 1.993, 0.061],
    [None, 15, 0.043, 0.042, 1.951, 0.060],
    [None, 16, 0.062, 0.045, 1.959, 0.057],
    [None, 17, 0.043, 0.078, 2.097, 0.100],
    [None, 18, 0.074, 0.047, 2.135, 0.059],
    [None, 19, 0.059, 0.045, 2.802, 0.056],
    [None, 20, 0.047, 0.045, 2.254, 0.057],

    ["Range Query (year 1990-2000)", 1, 6.938, 0.867, 19.136, 0.588],
    [None, 2, 7.444, 1.030, 18.090, 0.590],
    [None, 3, 7.860, 0.736, 15.745, 0.618],
    [None, 4, 6.664, 0.842, 14.788, 0.697],
    [None, 5, 7.396, 0.747, 17.416, 0.592],
    [None, 6, 8.142, 0.772, 15.915, 0.648],
    [None, 7, 7.158, 0.774, 17.082, 0.693],
    [None, 8, 6.833, 0.857, 15.503, 0.514],
    [None, 9, 7.171, 0.742, 16.426, 0.197],
    [None, 10, 7.983, 0.916, 14.693, 0.240],
    [None, 11, 7.393, 0.712, 15.355, 0.190],
    [None, 12, 6.764, 0.825, 16.742, 0.211],
    [None, 13, 6.891, 0.750, 14.403, 0.208],
    [None, 14, 9.643, 0.740, 16.313, 0.144],
    [None, 15, 6.933, 0.784, 15.052, 0.165],
    [None, 16, 6.799, 0.695, 14.256, 0.109],
    [None, 17, 6.886, 0.663, 17.265, 0.108],
    [None, 18, 7.419, 0.691, 14.875, 0.167],
    [None, 19, 6.970, 0.749, 15.459, 0.110],
    [None, 20, 6.197, 0.730, 14.737, 0.109],

    ["Title Update Operation", 1, 88.504, 102.908, 51.816, 2.859],
    [None, 2, 103.577, 111.829, 31.547, 1.149],
    [None, 3, 112.215, 118.216, 26.786, 1.122],
    [None, 4, 111.253, 126.362, 28.443, 1.205],
    [None, 5, 112.852, 126.989, 23.825, 1.240],
    [None, 6, 107.318, 125.700, 27.852, 1.088],
    [None, 7, 107.059, 113.512, 23.508, 1.074],
    [None, 8, 112.611, 109.114, 23.639, 0.316],
    [None, 9, 106.055, 116.496, 26.016, 0.347],
    [None, 10, 105.378, 120.751, 24.135, 0.353],
    [None, 11, 101.401, 121.316, 23.749, 0.391],
    [None, 12, 100.868, 122.236, 23.941, 0.542],
    [None, 13, 104.010, 119.173, 25.783, 0.267],
    [None, 14, 97.795, 118.818, 23.130, 0.244],
    [None, 15, 99.486, 122.998, 24.024, 0.316],
    [None, 16, 90.753, 116.196, 23.245, 0.539],
    [None, 17, 93.278, 120.298, 25.735, 0.353],
    [None, 18, 89.753, 121.669, 23.031, 0.299],
    [None, 19, 90.500, 111.461, 23.323, 0.310],
    [None, 20, 86.823, 116.656, 23.579, 0.388]
]

columns = ["test_type", "round_num", "db1_time_ms", "db2_time_ms", "file_time_ms", "memory_time_ms"]
df = pd.DataFrame(raw_data, columns=columns)
df["test_type"] = df["test_type"].fillna(method="ffill")
df["data_size"] = 10000

time_cols = ["db1_time_ms", "db2_time_ms", "file_time_ms", "memory_time_ms"]
for col in time_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

print("=== Raw Data Construction Completed (10000 Records) ===")
print(f"Total Rounds: {len(df)} (4 test types × 20 rounds)")
print(f"Test Type Distribution: {df['test_type'].value_counts().to_dict()}")
print("\n")


df["total_time_avg"] = df[time_cols].mean(axis=1)

def remove_outliers(group):
    """Remove 2 smallest and 2 largest rounds by total_time_avg for each test type"""
    if len(group) < 4:
        return group
    sorted_group = group.sort_values("total_time_avg")
    return sorted_group.iloc[2:-2] 

df_cleaned = df.groupby("test_type", group_keys=False).apply(remove_outliers)
df_cleaned = df_cleaned.drop(columns=["total_time_avg"]) 


print("=== Data Cleaning Comparison (10000 Records) ===")
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

print("=== Performance Statistics (10000 Records After Outlier Removal) ===")
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
fig, ax = plt.subplots(figsize=(12, 8))


bars1 = ax.bar(x - 1.5*width, df_mean["db1_time_ms"], width, label="DB1 (DB_VS_FILE)", color="#1f77b4")
bars2 = ax.bar(x - 0.5*width, df_mean["db2_time_ms"], width, label="DB2 (DB_VS_MEMORY)", color="#ff7f0e")
bars3 = ax.bar(x + 0.5*width, df_mean["file_time_ms"], width, label="File Storage", color="#2ca02c")
bars4 = ax.bar(x + 1.5*width, df_mean["memory_time_ms"], width, label="Memory Storage", color="#d62728")


ax.set_title("10000 Records (After Outlier Removal): Storage Time Comparison by Test Type", fontsize=16, pad=20)
ax.set_xlabel("Test Type", fontsize=12)
ax.set_ylabel("Average Time (ms)", fontsize=12)
ax.set_xticks(x)
ax.set_xticklabels(df_mean["test_type_simplified"], rotation=0)
ax.legend(fontsize=10)


def add_labels(bars):
    for bar in bars:
        h = bar.get_height()
        if h > 0.1:
            ax.text(bar.get_x() + bar.get_width()/2, h, f"{h:.3f}", ha="center", va="bottom", fontsize=9)

add_labels(bars1)
add_labels(bars2)
add_labels(bars3)
add_labels(bars4)


plt.tight_layout()
plt.savefig("10000_records_storage_time_comparison_cleaned.png", dpi=300, bbox_inches="tight")
plt.close()


fig, axes = plt.subplots(2, 2, figsize=(14, 10))
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

    ax.set_title(f"{test_type_map[test_type]} (After Outlier Removal)", fontsize=12)
    ax.set_ylabel("Time (ms)", fontsize=10)
    ax.grid(True, alpha=0.3)


fig.suptitle("10000 Records: Storage Time Distribution by Test Type (Box Plot)", fontsize=16, y=0.98)
plt.tight_layout()
plt.savefig("10000_records_time_distribution_boxplot_cleaned.png", dpi=300, bbox_inches="tight")
plt.close()


print("=== Core Conclusions (10000 Records After Outlier Removal) ===")

fuzzy_stats = stats_cleaned.loc["Fuzzy Query (title contains 'the')"]
exact_stats = stats_cleaned.loc["Exact Query (movie ID=1000)"]
range_stats = stats_cleaned.loc["Range Query (year 1990-2000)"]
update_stats = stats_cleaned.loc["Title Update Operation"]