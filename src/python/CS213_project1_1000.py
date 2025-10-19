import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

raw_data = [
    ["Fuzzy Query (title contains 'the')", 1, 4.488, 1.059, 30.359, 0.483],
    [None, 2, 1.366, 0.852, 15.007, 0.126],
    [None, 3, 0.791, 0.749, 8.433, 0.358],
    [None, 4, 0.915, 0.934, 2.936, 0.093],
    [None, 5, 0.831, 0.920, 3.135, 0.122],
    [None, 6, 0.868, 0.816, 4.656, 0.088],
    [None, 7, 0.920, 0.883, 4.143, 0.077],
    [None, 8, 0.820, 0.867, 2.163, 0.133],
    [None, 9, 0.821, 0.818, 2.139, 0.077],
    [None, 10, 0.767, 1.014, 2.605, 0.093],
    [None, 11, 0.833, 0.777, 2.358, 0.077],
    [None, 12, 0.756, 0.972, 2.581, 0.105],
    [None, 13, 0.801, 0.787, 2.458, 0.086],
    [None, 14, 0.780, 0.721, 2.530, 0.191],
    [None, 15, 1.038, 0.837, 3.161, 0.079],
    [None, 16, 0.837, 0.974, 2.596, 0.080],
    [None, 17, 0.787, 0.890, 2.363, 0.079],
    [None, 18, 0.748, 0.857, 2.397, 0.114],
    [None, 19, 0.854, 0.943, 2.828, 0.103],
    [None, 20, 1.013, 1.078, 2.558, 0.072],

    ["Exact Query (movie ID=1000)", 1, 0.366, 0.044, 2.400, 0.005],
    [None, 2, 0.044, 0.041, 2.522, 0.003],
    [None, 3, 0.041, 0.046, 2.407, 0.004],
    [None, 4, 0.044, 0.043, 2.591, 0.011],
    [None, 5, 0.039, 0.038, 2.424, 0.003],
    [None, 6, 0.039, 0.043, 2.617, 0.003],
    [None, 7, 0.041, 0.041, 2.691, 0.003],
    [None, 8, 0.042, 0.042, 2.453, 0.003],
    [None, 9, 0.042, 0.040, 2.207, 0.003],
    [None, 10, 0.044, 0.043, 2.134, 0.003],
    [None, 11, 0.038, 0.043, 2.061, 0.004],
    [None, 12, 0.072, 0.044, 2.813, 0.003],
    [None, 13, 0.059, 0.045, 3.464, 0.004],
    [None, 14, 0.044, 0.042, 1.888, 0.003],
    [None, 15, 0.039, 0.091, 2.119, 0.006],
    [None, 16, 0.041, 0.042, 2.096, 0.003],
    [None, 17, 0.039, 0.042, 2.766, 0.011],
    [None, 18, 0.042, 0.042, 2.482, 0.040],
    [None, 19, 0.041, 0.043, 3.107, 0.044],
    [None, 20, 0.047, 0.043, 2.319, 0.004],

    ["Range Query (year 1990-2000)", 1, 1.036, 0.827, 2.200, 0.090],
    [None, 2, 0.883, 0.789, 1.891, 0.071],
    [None, 3, 1.104, 0.896, 2.436, 0.072],
    [None, 4, 0.859, 0.738, 2.368, 0.093],
    [None, 5, 0.788, 0.846, 2.512, 0.054],
    [None, 6, 1.276, 0.852, 2.583, 0.086],
    [None, 7, 1.083, 0.960, 2.324, 0.074],
    [None, 8, 0.814, 0.788, 3.069, 0.085],
    [None, 9, 0.767, 1.049, 2.463, 0.084],
    [None, 10, 0.838, 0.897, 2.698, 0.121],
    [None, 11, 0.863, 0.855, 2.483, 0.142],
    [None, 12, 1.009, 0.708, 2.407, 0.150],
    [None, 13, 0.802, 0.779, 2.237, 0.086],
    [None, 14, 0.910, 0.737, 4.726, 0.093],
    [None, 15, 0.769, 0.847, 2.185, 0.085],
    [None, 16, 1.073, 0.939, 1.953, 0.071],
    [None, 17, 0.871, 0.858, 2.021, 0.073],
    [None, 18, 0.956, 0.996, 2.273, 0.071],
    [None, 19, 0.804, 0.798, 2.360, 0.112],
    [None, 20, 0.768, 0.875, 2.486, 0.075],

    ["Title Update Operation", 1, 8.425, 1.719, 9.612, 0.247],
    [None, 2, 3.061, 1.812, 8.165, 0.190],
    [None, 3, 3.075, 2.058, 6.348, 0.181],
    [None, 4, 3.364, 1.915, 5.489, 0.130],
    [None, 5, 3.361, 1.960, 6.063, 0.125],
    [None, 6, 3.074, 1.932, 5.590, 0.164],
    [None, 7, 4.143, 1.979, 5.153, 0.165],
    [None, 8, 2.591, 1.895, 5.187, 0.119],
    [None, 9, 2.507, 2.221, 4.880, 0.121],
    [None, 10, 2.230, 1.993, 4.949, 0.120],
    [None, 11, 2.459, 1.965, 5.270, 0.138],
    [None, 12, 1.892, 1.875, 4.912, 0.120],
    [None, 13, 2.684, 1.966, 5.857, 0.133],
    [None, 14, 1.922, 1.846, 4.979, 0.116],
    [None, 15, 1.895, 1.983, 4.637, 0.122],
    [None, 16, 2.023, 2.085, 4.756, 0.125],
    [None, 17, 1.892, 1.928, 6.472, 0.145],
    [None, 18, 1.965, 1.823, 4.909, 0.120],
    [None, 19, 1.931, 1.958, 4.855, 0.124],
    [None, 20, 1.810, 1.996, 4.666, 0.121]
]

columns = ["test_type", "round_num", "db1_time_ms", "db2_time_ms", "file_time_ms", "memory_time_ms"]
df = pd.DataFrame(raw_data, columns=columns)
df["test_type"] = df["test_type"].fillna(method="ffill")
df["data_size"] = 1000

time_cols = ["db1_time_ms", "db2_time_ms", "file_time_ms", "memory_time_ms"]
for col in time_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce")

print("=== Raw Data Construction Completed ===")
print(f"Total Rounds: {len(df)} (4 test types × 20 rounds)")
print(f"Test Type Distribution: {df['test_type'].value_counts().to_dict()}")
print("\n")


df["total_time_avg"] = df[time_cols].mean(axis=1)

def remove_outliers(group):
    """Remove top 2 and bottom 2 rounds with extreme total_time_avg for each test_type group"""
    if len(group) < 4:
        return group
    sorted_group = group.sort_values("total_time_avg")
    return sorted_group.iloc[2:-2]

df_cleaned = df.groupby("test_type", group_keys=False).apply(remove_outliers)
df_cleaned = df_cleaned.drop(columns=["total_time_avg"]) 

print("=== Data Cleaning Comparison (1000 Records) ===")
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

print("=== Performance Statistics (1000 Records After Outlier Removal) ===")
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

bars1 = ax.bar(x - 1.5 * width, df_mean["db1_time_ms"], width, label="DB1 (DB_VS_FILE)", color="#1f77b4")
bars2 = ax.bar(x - 0.5 * width, df_mean["db2_time_ms"], width, label="DB2 (DB_VS_MEMORY)", color="#ff7f0e")
bars3 = ax.bar(x + 0.5 * width, df_mean["file_time_ms"], width, label="File Storage", color="#2ca02c")
bars4 = ax.bar(x + 1.5 * width, df_mean["memory_time_ms"], width, label="Memory Storage", color="#d62728")

ax.set_title("1000 Records (After Outlier Removal): Storage Method Time Comparison by Test Type", fontsize=16, pad=20)
ax.set_xlabel("Test Type", fontsize=12)
ax.set_ylabel("Average Time (ms)", fontsize=12)
ax.set_xticks(x)
ax.set_xticklabels(df_mean["test_type_simplified"], rotation=0)
ax.legend(fontsize=10)


def add_labels(bars):
    for bar in bars:
        h = bar.get_height()
        if h > 0.05:
            ax.text(bar.get_x() + bar.get_width() / 2, h, f"{h:.3f}", ha="center", va="bottom", fontsize=9)


add_labels(bars1)
add_labels(bars2)
add_labels(bars3)
add_labels(bars4)

plt.tight_layout()
plt.savefig("1000_records_storage_time_comparison_cleaned.png", dpi=300, bbox_inches="tight")
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

fig.suptitle("1000 Records: Storage Method Time Distribution by Test Type (Box Plot)", fontsize=16, y=0.98)
plt.tight_layout()
plt.savefig("1000_records_time_distribution_boxplot_cleaned.png", dpi=300, bbox_inches="tight")
plt.close()


print("=== Core Conclusions (1000 Records After Outlier Removal) ===")

fuzzy_stats = stats_cleaned.loc["Fuzzy Query (title contains 'the')"]
exact_stats = stats_cleaned.loc["Exact Query (movie ID=1000)"]
range_stats = stats_cleaned.loc["Range Query (year 1990-2000)"]
update_stats = stats_cleaned.loc["Title Update Operation"]