import pandas as pd

# CSV 파일 불러오기
df1 = pd.read_csv("reviews_letterboxd_oldest.csv")
df2 = pd.read_csv("reviews_letterboxd_newest.csv")

# 두 데이터프레임 합치기 (행 기준)
df_all = pd.concat([df1, df2], ignore_index=True)

# 무작위로 1000개 행 샘플링
df_sample = df_all.sample(n=1000, random_state=42)

# 새 CSV 파일로 저장
df_sample.to_csv("reviews_letterboxd.csv", index=False)