# ETL_Pipeline
## ETL 파이프라인
![image](https://user-images.githubusercontent.com/48642518/225624054-e1af5b6f-00ec-4336-81e5-ff13eeb811fa.png)

### ETL -> Extract, Transform, Load의 축약어로 data 분석을 위해 data warehouse, data mart 내로 이동시키는 과정
* Extract : 소스 data로부터 추출
* Transform : DeNomalize 등의 추출된 데이터 변형
* Load : DW(DataWarehouse)로의 데이터 적재

## 프로젝트 목표
* ETL 파이프라인 구축
  1. 데이터 서버에서 스케줄링을 통해 가져온 데이터를 변환하고 암호화하여 100개 단위로 압축
  2. 압축한 데이터를 aws s3에 적제
  3. 적재된 압축 파일 다시 가져와서 복원

## 프로젝트 수행 과정
![image](https://user-images.githubusercontent.com/48642518/227080428-bab1cbb6-b697-4bb7-bac9-e1535263eb15.png)
1. extract - 서버에서 데이터를 가져온다.
2. transform - 암호화된 데이터를 복호화한 후 문자열을 변환 -> 변환된 데이터를 암호화하고 다시 압축
3. load - 압축된 데이터를 aws s3에 dynamic partitioning을 통해 년도, 날짜, 시간, 분 단위로 디렉토리를 구분해서 적재


## 업로드 수행 결과
![image](https://user-images.githubusercontent.com/48642518/227080143-44aa5158-9459-4823-86d8-cc44bab9d51b.png)

## 적재 데이터 다운로드 후 복원 수행 결과
* 파이썬 파일을 실행할 때 파일명을 매개변수로 넘겨주면 해당 파일을 aws s3에서 다운받은 후 암호화된 데이터를 복호화하고 복원

![image](https://user-images.githubusercontent.com/48642518/227154897-a1b5420c-a11c-4e25-a5dc-5a38a3a4aab7.png)

* s3에서 데이터를 가지고 온 후

![image](https://user-images.githubusercontent.com/48642518/227154738-6d280136-8c6c-40a9-8eb6-a9259e8a1a25.png)


## 개선 할 부분(2023-03-29)
1. ~~적재된 데이터를 다운로드 후 복원~~(완료/ 2023-03-23)
2. ~~함수 모듈화~~(완료 / 2023-03-23)
3. 함수 예외처리 필요 (일부 진행 / 2023-03-29) 
4. aws athena를 이용하여 데이터 조회
