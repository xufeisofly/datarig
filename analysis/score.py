import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy.ndimage import uniform_filter1d
from numpy.polynomial import Polynomial

# 读取 CSV 文件
df = pd.read_csv('v2_with_score.csv')

# 按照 score 从低到高排序
df = df.sort_values(by='score', ascending=True)

# 提取需要的列
x = range(len(df))  # 横坐标为每一条数据的索引
score = df['score']
y1 = df['line_punct_ratio']
y2 = df['high_quality_ratio_10']
y3 = df['short_line_ratio']

# 找到score=0.018112与score曲线相交点对应的横坐标
score_target = 0 # 0.018112
# 计算所有大于等于score_target的第一个索引
idx_candidates = np.where(score >= score_target)[0]
if len(idx_candidates) > 0:
    idx = idx_candidates[0]
else:
    idx = len(score) - 1  # 如果都小于target，取最后一个

# 获取该点对应的其他指标阈值
y1_th = y1.iloc[idx]
y2_th = y2.iloc[idx]
y3_th = y3.iloc[idx]

# 对 ratio 指标做平滑处理（滑动平均）
window_size = 15  # 可根据实际情况调整
smooth_y1 = uniform_filter1d(y1, size=window_size)
smooth_y2 = uniform_filter1d(y2, size=window_size)
smooth_y3 = uniform_filter1d(y3, size=window_size)

# 获取平滑后在 idx 处的阈值
smooth_y1_th = smooth_y1[idx]
smooth_y2_th = smooth_y2[idx]
smooth_y3_th = smooth_y3[idx]

# 画折线图
plt.figure(figsize=(10, 6))
plt.plot(x, y1, label='line_punct_ratio (raw)', alpha=0.3)
plt.plot(x, y2, label='high_quality_ratio_10 (raw)', alpha=0.3)
plt.plot(x, y3, label='short_line_ratio (raw)', alpha=0.3)
plt.plot(x, smooth_y1, label='line_punct_ratio (smooth)', linewidth=2)
plt.plot(x, smooth_y2, label='high_quality_ratio_10 (smooth)', linewidth=2)
plt.plot(x, smooth_y3, label='short_line_ratio (smooth)', linewidth=2)
plt.plot(x, score, label='score', linestyle='--')

# 画竖线
plt.axvline(idx, color='red', linestyle=':', label='score=0.018112 index')
# 画横线
plt.axhline(score_target, color='red', linestyle=':', label='score=0.018112')

plt.xlabel('data index')
plt.ylabel('value')
plt.title('Data Index vs. Ratios and Score (Smoothed)')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

# 输出阈值
print(f'在score≈0.018112时，index={idx}')
print(f'line_punct_ratio阈值: {y1_th}')
print(f'high_quality_ratio_10阈值: {y2_th}')
print(f'short_line_ratio阈值: {y3_th}')
# 输出平滑后的阈值
print(f'在score≈0.018112时，index={idx}')
print(f'line_punct_ratio平滑阈值: {smooth_y1_th}')
print(f'high_quality_ratio_10平滑阈值: {smooth_y2_th}')
print(f'short_line_ratio平滑阈值: {smooth_y3_th}')



# 分段参数
num_segments = 100  # 可调参数，每段约100个点
segment_size = int(np.ceil(len(score) / num_segments))

# 计算每段最大/最小值点索引
max_idx_list_y1, min_idx_list_y1 = [], []
max_idx_list_y2, min_idx_list_y2 = [], []
max_idx_list_y3, min_idx_list_y3 = [], []
for i in range(num_segments):
    start = i * segment_size
    end = min((i + 1) * segment_size, len(score))
    # y1
    seg = smooth_y1[start:end]
    if len(seg) > 0:
        max_idx_list_y1.append(start + np.argmax(seg))
        min_idx_list_y1.append(start + np.argmin(seg))
    # y2
    seg = smooth_y2[start:end]
    if len(seg) > 0:
        max_idx_list_y2.append(start + np.argmax(seg))
        min_idx_list_y2.append(start + np.argmin(seg))
    # y3
    seg = smooth_y3[start:end]
    if len(seg) > 0:
        max_idx_list_y3.append(start + np.argmax(seg))
        min_idx_list_y3.append(start + np.argmin(seg))

# 生成最大/最小值连线曲线
max_curve_y1 = smooth_y1[max_idx_list_y1]
min_curve_y1 = smooth_y1[min_idx_list_y1]
max_curve_y2 = smooth_y2[max_idx_list_y2]
min_curve_y2 = smooth_y2[min_idx_list_y2]
max_curve_y3 = smooth_y3[max_idx_list_y3]
min_curve_y3 = smooth_y3[min_idx_list_y3]

# 横坐标为最大/最小点的索引
seg_x = np.array(max_idx_list_y1)

# 找到 score ≈ 0.018112 在分段曲线上的阈值
# 找到 seg_x 中第一个大于等于 idx 的位置
seg_idx = np.searchsorted(seg_x, idx, side='right') - 1
seg_idx = np.clip(seg_idx, 0, len(seg_x) - 1)

# 输出阈值
print(f'在score≈0.018112时，index={idx}, 分段索引={seg_idx}')
print(f'line_punct_ratio最大值连线阈值: {max_curve_y1[seg_idx]}')
print(f'line_punct_ratio最小值连线阈值: {min_curve_y1[seg_idx]}')
print(f'high_quality_ratio_10最大值连线阈值: {max_curve_y2[seg_idx]}')
print(f'high_quality_ratio_10最小值连线阈值: {min_curve_y2[seg_idx]}')
print(f'short_line_ratio最大值连线阈值: {max_curve_y3[seg_idx]}')
print(f'short_line_ratio最小值连线阈值: {min_curve_y3[seg_idx]}')

# 画图
plt.figure(figsize=(12, 7))
plt.plot(x, smooth_y1, label='line_punct_ratio (smooth)', alpha=0.5)
plt.plot(x, smooth_y2, label='high_quality_ratio_10 (smooth)', alpha=0.5)
plt.plot(x, smooth_y3, label='short_line_ratio (smooth)', alpha=0.5)
plt.plot(seg_x, max_curve_y1, 'r-', label='line_punct_ratio max-curve', linewidth=2)
plt.plot(seg_x, min_curve_y1, 'r--', label='line_punct_ratio min-curve', linewidth=2)
plt.plot(seg_x, max_curve_y2, 'g-', label='high_quality_ratio_10 max-curve', linewidth=2)
plt.plot(seg_x, min_curve_y2, 'g--', label='high_quality_ratio_10 min-curve', linewidth=2)
plt.plot(seg_x, max_curve_y3, 'b-', label='short_line_ratio max-curve', linewidth=2)
plt.plot(seg_x, min_curve_y3, 'b--', label='short_line_ratio min-curve', linewidth=2)
plt.plot(x, score, label='score', linestyle='--', color='gray', alpha=0.5)
plt.axvline(idx, color='black', linestyle=':', label='score=0.018112 index')
plt.axhline(score_target, color='black', linestyle=':', label='score=0.018112')
plt.xlabel('data index')
plt.ylabel('value')
plt.title(f'Data Index vs. Ratios and Score (Segmented Max/Min Curves, {num_segments} segments)')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()