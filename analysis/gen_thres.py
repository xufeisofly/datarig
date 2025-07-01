import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


# 读取 CSV 文件
df = pd.read_csv('v2_with_score.csv')

thresholds_score = 0
ratio_name = 'short_line_ratio'

# 增加 is_good 字段
df['is_good'] = df['score'] > thresholds_score

# 按 line_punct_ratio 从低到高排序
df = df.sort_values(by=ratio_name, ascending=True)

# 画 is_good 在不同 line_punct_ratio 上的直方图
plt.figure(figsize=(10, 6))
plt.hist(df[df['is_good']][ratio_name], bins=60, alpha=0.7, label='is_good=True')
plt.hist(df[~df['is_good']][ratio_name], bins=60, alpha=0.7, label='is_good=False')
plt.xlabel(ratio_name)
plt.ylabel('Count')
plt.title(f'Distribution of is_good by {ratio_name}')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()


# 提取两组数据
good = df[df['is_good']][ratio_name].values
bad  = df[~df['is_good']][ratio_name].values

# 排序
good_sorted = np.sort(good)
bad_sorted  = np.sort(bad)

# 候选阈值：这里直接用所有唯一值，也可以用 np.linspace(min, max, 1000) 做网格
thresholds = np.unique(df[ratio_name])

# 计算两组在每个阈值处的 CDF
# searchsorted 找到第一个大于阈值的位置，除以样本数即为 CDF
cdf_good = np.searchsorted(good_sorted, thresholds, side='right') / good_sorted.size
cdf_bad  = np.searchsorted(bad_sorted,  thresholds, side='right') / bad_sorted.size

# KS 统计量 = 两个 CDF 之差的绝对值
ks_stat = np.abs(cdf_good - cdf_bad)

# 找到最大 KS 的索引
best_idx       = np.argmax(ks_stat)
best_threshold = thresholds[best_idx]
best_ks_value  = ks_stat[best_idx]

print(f"最佳阈值 {ratio_name} = {best_threshold:.4f}")
print(f"对应的 KS 值 = {best_ks_value:.4f}")

# （可选）画出 KS 统计量随阈值变化的曲线，帮助做视觉确认
plt.figure(figsize=(8,4))
plt.plot(thresholds, ks_stat, label='KS statistic')
plt.axvline(best_threshold, color='red', linestyle='--',
            label=f'Best threshold = {best_threshold:.3f}')
plt.xlabel(f'{ratio_name} 阈值')
plt.ylabel('KS 统计量')
plt.title('KS 统计量 vs. 阈值')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()