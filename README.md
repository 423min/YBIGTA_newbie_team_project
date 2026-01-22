## *🔍 Web 과제 🔍*
> FastAPI와 MVC 패턴을 기반으로 사용자 로그인 기능 구현 과제
---
## 👀 OVERVIEW 

 1. index.html 꾸미기
 2. user_service.py 작성
 3. user_router.py 작성

## 💻 index.html

- 로그인 페이지의 사이트 화면을 깔끔한 파스텔 톤 파란색으로
- 사이트 가독성을 향상시키기 위해 배경과 어울리는 폰트를 지정
- 각 버튼의 역할에 맞는 색 지정 (Update는 파란색, Delete는 빨간색)
- 모든 요소들 가운데 정렬

## 💻 user_service.py

- 이메일을 찾지 못할 경우의 오류
- 비밀번호가 틀릴 경우의 오류
- 이메일이 존재할 경우의 오류

       #이메일이 존재할 경우 사용자 객체 정의
       exist_user = self.repo.get_user_by_email(new_user.email)
        
       if exist_user != None:
          raise ValueError("User already Exists.")

## 💻 user_router.py

- user의 register, delete, update password 기능을 구현
- 성공/에러 응답 반환

## *📑크롤링 과제📑*
> 크롤러를 통한 다양한 사이트의 리뷰 수집
---
## 👀 OVERVIEW 
주토피아1 리뷰 크롤링
## 🎬 RottenTomatoesCrawler
- 크롤링 링크: https://www.rottentomatoes.com/m/zootopia/reviews/all-audience
- 데이터 형식: rating(별 5개 기준, 1/2 가능), date, comment를 csv파일로 저장
- 데이터 개수: 1000개
> 크롬 브라우저 실행
> -> 날짜 문자열 파싱
> -> 리뷰 데이터 수집 - 'Load More' 버튼 클릭하여 페이지 넘김
> -> 수집 데이터 csv 파일 저장
## 🎬 LetterboxdCrawler
- 크롤링 링크: https://letterboxd.com/film/zootopia/reviews/
- 데이터 형식: rating(별 5개 기준, 1/2 가능), rating_raw, date, content를 csv파일로 저장
- 데이터 개수: 1000개
> 크롬 브라우저 실행
> -> 리뷰 데이터 수집 - 'Older' 버튼 클릭하여 페이지 넘김
> -> 수집 데이터 csv 파일 저장

> *타 사이트에 비해 리뷰 수가 현저히 많아서 newest 버전과 oldest 버전을 추출*

> *Letterboxd 사이트의 경우 캡챠가 발생하는 경우가 존재해서 우회하는 방식으로 코드 작성해두었습니다.*
## 🎬 IMDbCrawler
- 크롤링 링크: https://www.imdb.com/title/tt2948356/reviews/
- 데이터 형식: rating(10점 기준), date, content를 csv파일로 저장
- 데이터 개수: 1000개
> 크롬 브라우저 실행
> -> 타겟 스크랩 개수 설정
> -> 리뷰 데이터 수집 - 'Load More' 버튼 클릭하여 페이지 넘김
> -> 수집 데이터 csv 파일 저장

## 📋EDA/FE📋
> 크롤링한 데이터 기반으로 사이트 간 비교분석 수행
---
## 📊 EDA
1. Rating Distribution
- Rotten Tomatoes

  <img width="690" height="290" alt="image" src="https://github.com/user-attachments/assets/3aa150ae-0e9e-46fc-a850-f8fb2c43086f" />
- IMDb

  <img width="690" height="290" alt="image" src="https://github.com/user-attachments/assets/d7202e3c-eabc-4291-bb87-63ade799eb8c" />
- Letterboxd

  <img width="690" height="290" alt="image" src="https://github.com/user-attachments/assets/847b5708-2126-47c6-9371-cd5231daa9c5" />
> 세 사이트의 별점 분포에 대한 그래프이다. IMDb는 10점 기준, Rotten Tomatoes와 Letterboxd는 5점 기준이며 1/2점이 존재한다. 10점 만점 기준 8점부터 급격히 비율이 상승하는 유사한 양상을 보인다.
2. Montly Count
- Rotten Tomatoes

  <img width="2990" height="990" alt="image" src="https://github.com/user-attachments/assets/5fbf3155-ed08-4f29-8d41-80b3ad6827fd" />
- IMDb

  <img width="2990" height="990" alt="image" src="https://github.com/user-attachments/assets/6c221f9d-ba6f-47dc-b9b1-588149dd9681" />
- Letterboxd

  <img width="2990" height="989" alt="image" src="https://github.com/user-attachments/assets/16ce0cd8-74bd-467d-a0ed-324de11c6838" />
> 세 사이트의 월별 리뷰수에 대한 그래프이다.
3. Text length distribution
- Rotten Tomatoes

  <img width="690" height="290" alt="image" src="https://github.com/user-attachments/assets/0576e996-c958-44ae-9506-f6af28b5fb9d" />
- IMDb

  <img width="689" height="290" alt="image" src="https://github.com/user-attachments/assets/e3784495-c853-45c7-8a61-5536a5f4ee4b" />
- Letterboxd

  <img width="688" height="290" alt="image" src="https://github.com/user-attachments/assets/09afaffb-8668-46e8-83c5-c0b5d739373a" />
> 세 사이트의 리뷰 길이에 대한 그래프이다. 대체로 짧은 길이의 리뷰가 많은 비중을 차지하며, Letterboxd의 경우가 비교적 짧은 리뷰들이 주를 이룬다는 것을 알 수 있다.
## 📊 전처리/FE 결과 설명
1. 전처리
- 수집된 영화 리뷰 데이터는 사이트별로 형식과 품질이 달라, 분석에 앞서 공통 기준에 맞게 전처리를 수행

- 리뷰 내용이 비어 있거나 평점·날짜 정보가 누락된 데이터 제거
의미 있는 텍스트 분석을 위해 리뷰 단어 수가 지나치게 짧은 데이터 제외

- 리뷰 텍스트 소문자 변환, 특수문자 제거, 불용어 제거, lemmatization
=> 단어 형태를 통일

- langdetect를 활용하여 영어가 아닌 리뷰는 제거하여 언어 차이로 인한 분석 왜곡 방지

- 추가적으로 letterboxd의 경우 리뷰 수가 너무 많아서 oldest, newest 기준으로 1000개씩 추출 후 두 데이터 랜덤 추출해서 토탈 1000개 데이터 추출

2. F/E
- 분석 목적에 맞는 파생 변수 생성

- 리뷰 작성 시점의 월 단위 분석을 위해 date 컬럼을 기반으로 year_month 변수 생성
  => 시간 흐름에 따른 리뷰 특성 변화 분석

- 정제된 리뷰 텍스트의 단어 수를 나타내는 clean_word_count 변수 생성
=> 사이트별 리뷰 길이 차이 및 리뷰 스타일 비교 가능

* TF-IDF 텍스트 벡터화 및 가중치 생성 *

* 주관성 점수 subjectivity_score 산출 *
  
   1. 어휘 사전 구축: MPQA Subjectivity Lexicon을 기반으로 영화 리뷰 도메인에 적합한 단어들을 필터링하고 주관성 강도(Strong/Very Strong)별 가중치 부여
   2. 계산식: Sum(단어별 TF-IDF 값 * 사전 가중치) / 정제된 리뷰 단어 수 (리뷰 길이에 따른 편향 제거)
## 📊 사이트 간 비교분석 결과
- 주관성 분포 비교

  <img width="800" height="500" alt="boxplot" src="https://github.com/user-attachments/assets/ad365f0b-f55a-4bad-b561-45070aae544a" />
> boxplot을 확인해보면 IMDb < Letterboxd < RottenTomatoes의 순으로 주관성 점수의 중앙값이 크다는 사실을 파악할 수 있었다. 즉, 해당 순서대로 점점 주관적인 리뷰가 많다는 것을 알 수 있다. IMDb는 전반적으로 설명적이며 객관적인 리뷰가 많고, Letterboxd는 감정과 비평의 혼합형의 리뷰가 많으며, RottenTomatoes의 경우에는 주관적이며 감정 표현이 뚜렷한 리뷰의 비중이 높다는 것을 알 수 있다. IMDb의 경우 박스가 좁은 것으로 보아 리뷰 스타일이 비교적 균질적이라는 것을 알 수 있다.
- 핵심 키워드 비교

   <img width="1600" height="1200" alt="top_15_lexicon_words_IMDB" src="https://github.com/user-attachments/assets/f01ee39b-a56d-49a4-b90c-d48886420d20" />
   <img width="1600" height="1200" alt="top_15_lexicon_words_Letterboxd" src="https://github.com/user-attachments/assets/a0795e25-175a-4ed1-b026-c05a3ee4c554" />
   <img width="1600" height="1200" alt="top_15_lexicon_words_RottenTomatoes" src="https://github.com/user-attachments/assets/d8cf7e4e-d9ab-4a20-bed9-4d952588932b" />
 > 비교분석을 위해 먼저 lexicon 사전을 직접 정의하였다. lexicon 사전에 주관성을 나타내줄 수 있는 단어들을 정의해두었다. 이를 통해 관측하였을 때, IMDb에서 주관성을 나타내는 데에 가장 많은 영향을 미친 단어는 like, great, good 등이 있다. Letterboxd에서도 마찬가지로 like, good, great 순으로 영향을 미쳤지만 like의 비중이 매우 높은 것을 확인할 수 있다. RottenTomatoes에서는 great, good, best 순으로 영향을 미쳤으며, 타 사이트에서 1순위였던 like가 4순위인 것을 확인할 수 있었다.
- 주토피아2 개봉 시점 리뷰 변화

  <img width="3600" height="1800" alt="daily_count_IMDB" src="https://github.com/user-attachments/assets/433b14e9-b5de-4880-922b-8cdb0071aa31" />
  <img width="3600" height="1800" alt="daily_count_Letterboxd" src="https://github.com/user-attachments/assets/d6d7cf01-78b3-4aa4-9336-a65ab89e9715" />
  <img width="3600" height="1800" alt="daily_count_RottenTomatoes" src="https://github.com/user-attachments/assets/ff368597-b622-43c3-9345-37ec35b57ee8" />
> 주토피아2 개봉 시점인 25년 11월 26일을 기준으로 리뷰 수의 시간적 변화를 관찰하였다. 세 사이트 모두 개봉 시점을 전후로 리뷰 활동이 집중되는 양상을 보였다. 특히 개봉 직후의 리뷰 수가 가장 높으며, 이후에는 급격하게 감소하기 시작한다. IMDb의 경우에는 개봉 초기에는 비교적 높은 리뷰 수가 관측되지만 이후에는 장기간에 걸쳐 낮은 수준의 리뷰 수가 유지되었다. RottenTomatoes의 경우 개봉 시점 등 특정 시점에서는 뚜렷하게 리뷰 수가 많지만 장기적으로는 낮은 수준에서 지속적으로 리뷰가 생성된다. 즉, 두 사이트 모두 주토피아2 라는 특정한 이벤트가 발생하였을 때 리뷰수가 급격히 증가하는 양상을 보이고 있다.
## *👥 GitHub 협업 과제 👥*
> Git 협업 규칙 설정하기
---
## 🪪 팀 소개
5조 안재후, 이근하, 변민주(조장)
- 안재후: 컴퓨터공학과 21
- 이근하: 도시공학과 22 
- 변민주: 인공지능학과 24

## 🪪 Git 설정
- branch_protection

  <img width="997" height="166" alt="image" src="https://github.com/user-attachments/assets/251c0cc7-be39-485e-8494-6f5275b0befc" />
- push_rejected

  <img width="868" height="351" alt="image" src="https://github.com/user-attachments/assets/f1318fe3-0fcf-4463-a3cd-a60223dcfca5" />
- review_and_merged
  
  <img width="569" height="709" alt="스크린샷 2026-01-22 134601" src="https://github.com/user-attachments/assets/e1c5a021-aee5-4312-a9b3-d8d403f619e5" />
