# ETL_Pipeline 구축
## Ver 1.0
1. API 서버에서 생성되는 암호화된 로그 데이터를 추출
2. 추출한 데이터를 복호화 및 전처리한 후 압축
3. 압축한 데이터를 AWS S3에 dynamic partitioning 방식으로 업로드

* [프로젝트 README.md](https://github.com/hyeon136/ETL_Pipeline/blob/main/ETL_pipeline/README.md)

---
## Ver 2.0
1. Airflow를 사용하여 일정 시간마다 추출, 변환, 적재가 실행되도록 스케줄링
2. API 서버에서 생성되는 암호화된 로그 데이터를 추출하여 .json 파일로 저장
3. 저장된 데이터를 pyspark로 읽어와 데이터 복호화 및 전처리한 후 parquet 형식으로 저장
4. 저장된 parquet 파일을 AWS S3에 dynamic partitioning 방식으로 업로드
