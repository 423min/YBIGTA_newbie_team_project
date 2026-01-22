import pandas as pd
import matplotlib.pyplot as plt

# 1) 파일 경로
path_imdb = r"C:\Users\gha52\OneDrive\YBIGTA\YBIGTA_newbie_team_project\database\preprocessed_reviews_imdb.csv"
path_letterboxd = r"C:\Users\gha52\OneDrive\YBIGTA\YBIGTA_newbie_team_project\database\preprocessed_reviews_letterboxd.csv"
path_rotten = r"C:\Users\gha52\OneDrive\YBIGTA\YBIGTA_newbie_team_project\database\preprocessed_reviews_RottenTomatoes.csv"

# 2) CSV 로드
df_imdb = pd.read_csv(path_imdb)
df_letterboxd = pd.read_csv(path_letterboxd)
df_rotten = pd.read_csv(path_rotten)

# 3) subjectivity_score만 뽑아서 박스플롯용 리스트로 준비 (결측 제거)
data = [
    df_imdb["subjectivity_score"].dropna(),
    df_letterboxd["subjectivity_score"].dropna(),
    df_rotten["subjectivity_score"].dropna()
]
labels = ["IMDb", "Letterboxd", "RottenTomatoes"]

# 4) 박스플롯 그리기
plt.figure(figsize=(8, 5))
plt.boxplot(data, labels=labels)
plt.title("Subjectivity Score Distribution by Platform")
plt.xlabel("Platform")
plt.ylabel("Subjectivity Score")
plt.tight_layout()
plt.show()
