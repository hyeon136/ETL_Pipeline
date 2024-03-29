# ETL_Pipeline Ver 1.5
## 프로젝트 개요
* ETL-Pipeline Ver 1.0에서 만든 파이프라인은 APScheduler를 사용하여 모니터링이 어려움
* 이를 해결하기 위해 Airflow를 도입하여 DAG를 구성하여 스케줄링 실행

## 문제점
1. AWS Freetier t2.micro의 할당된 자원이 적어 Airflow Webserver는 작동하지만 Scheduler를 키면 멈추는 현상이 발생
2. Airflow는 task가 독립적으로 실행되기 때문에 기본적으로는 서로 통신이 불가하여 추출한 데이터를 읽어와서 변환할 수 없음

## 1. 해결방안
* t3a.medium 인스턴스로 변경하여 수행

## 2. 해결방안
* XCom을 이용하여 task간의 메세지를 교환
* XCom의 MAX Size는 48KB이므로 추출한 json 데이터를 전송하기 어려워 추출한 데이터를 임시로 json 파일로 저장
* 저장한 파일명을 XCom을 이용하여 Transform task로 전달하여 저장한 json 데이터를 읽도록 수행
* Tranform task에서도 위와 동일하게 압축하여 저장한 파일명을 Load task로 전달



---
# ETL_Pipeline Ver 2.0
## 프로젝트 개요
* ETL-Pipeline Ver 1.5에서 만든 파이프라인은 데이터의 양이 많아질수록 많은 시간을 소요하여 빠른 처리와 자동화를 위해 Airflow와 Pyspark를 이용하여 많은 양의 데이터를 수집과 처리, 적재를 자동화
* spark는 싱글 노드로 사용

## 프로젝트 구조
![image](https://user-images.githubusercontent.com/48642518/233578199-b2ca34fe-56db-43d8-8a23-650b8ecb7a64.png)

1. Airflow에서 작성해둔 DAG를 실행시킨다.
2. API 서버에서 암호화된 로그 데이터를 추출, 추출한 데이터를 .json 파일로 저장
3. 저장한 데이터를 spark를 통해 처리 후 parquet 형식으로 저장
4. 저장된 parquet 파일을 AWS S3에 Dynamic Partitoning 방식으로 년/월/일/시간 순서로 디렉토리를 만들어 적재

## 수행 결과
### Extract
![image](https://user-images.githubusercontent.com/48642518/233581264-a36a4897-138a-4375-af75-b002ea95d704.png)
![image](https://user-images.githubusercontent.com/48642518/233581312-f04c84fa-0e55-4088-a606-2e71e0dd0345.png)

1. API 서버에 요청을 보내 가져온 데이터를 json 형식으로 변환
2. 가져온 데이터를 tmp.json으로 저장
3. 542KB 크기의 json 파일 생성

### Transform
![image](https://user-images.githubusercontent.com/48642518/233583218-bcad70c2-0434-4d36-9a50-fcd81a030751.png)
![image](https://user-images.githubusercontent.com/48642518/233584028-cdd2e2e5-1ecb-4a70-8703-ec6356c4eb87.png)

1. pyspark를 통해 json 파일 read
2. 읽어온 파일에서 사용하지 않는 recorId, ArrivalTimeStamp 컬럼 제거
3. 암호화되어 있는 data 컬럼 복호화 수행

![image](https://user-images.githubusercontent.com/48642518/233585362-2f288ede-f968-4512-aa93-764943732177.png)

1. 복호화된 data 컬럼에 모든 json 내용이 있으므로 컬럼을 분리

![image](https://user-images.githubusercontent.com/48642518/233585571-58b2ff81-ef41-486b-9d1e-bbd40760a9a6.png)
1. user_id - b64uuid 모듈을 사용하여 64자리였던 user_id를 44자리로 축소
2. method, url - 공통적인 문자열 숫자 데이터로 맵핑
3. inDate - 날짜 데이터에서 숫자를 제외한 나머지 문자를 제거하고 반환

![image](https://user-images.githubusercontent.com/48642518/233586042-c504a329-7814-42ed-b899-12df5c20854a.png)
![image](https://user-images.githubusercontent.com/48642518/233586484-48900c99-0317-4bf4-a695-c1b5f25ee394.png)

1. inDate 컬럼을 사용하여 시간 데이터를 꺼내어 생성 시간이 다른 행을 df1과 df2로 분리
2. parquet 형식으로 압축하여 저장
3. 원본 json 파일 크기인 542KB보다 약 90% 작아진 54KB크기를 가진다.

### Load
![image](https://user-images.githubusercontent.com/48642518/233587394-4ac1d2de-35de-4820-9914-9ba739461a87.png)
![image](https://user-images.githubusercontent.com/48642518/233587454-e0d17cd8-e8b9-4218-b648-83ebd85bebb1.png)
![image](https://user-images.githubusercontent.com/48642518/233587508-bf2d31a6-500c-4538-b639-dfc48292d775.png)

1. data 디렉토리 하위에 년/월/일/시간 순으로 디렉토리가 생성되고 parquet 폴더 업로드

### 개선할 부분
* 싱글 노드를 사용한 spark를 클러스터 구성하여 데이터 처리
