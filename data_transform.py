import os
import gzip
import boto3
import datetime
from dotenv import load_dotenv
from apscheduler.schedulers.background import BlockingScheduler
load_dotenv("./key.env")

def request_url(url):
    import requests

    return requests.get(url).json()

#json의 ['data']['method'] 매핑
def method_mapping(data):
    mapping_dict = {"GET" : 1, "POST" : 2}
    
    return mapping_dict.get(data, 0)

#json의 ['data']['url'] 매핑
def url_mapping(data):
    mapping_dict = {"api" : 0, "users" : 1, "products" : 2, "data" : 3, "product" : 4, "log" : 5}

    # / 를 기준으로 데이터를 나눔
    tmp = data.split('/')
    # 리스트에 있는 공백 제거
    tmp = [s for s in tmp if s]
    result = []

    for i in tmp:
        result.append(mapping_dict.get(i, -1))

    return result

#[data][user_id] -> b64uuid로 변환
def convert_uuid(id):
    import b64uuid

    return b64uuid.B64UUID(id[:32]).string + b64uuid.B64UUID(id[32:]).string

#날짜 형식 숫자만 남김
def convert_inDate(date:str):
    import re
    
    return re.sub("[^0-9]", "", date[2:])

def str_to_json(str):
    import json

    return json.loads(str.replace("'","\""))

def decrypt_data(data):
    from cryptography.fernet import Fernet
    
    key = b't-jdqnDewRx9kWithdsTMS21eLrri70TpkMq2A59jX8='
    fernet = Fernet(key)

    return fernet.decrypt(data).decode('ascii')

#데이터 복호화
def decrypt(data):
    #decrypt시 bytes 객체로 리턴되어 decode
    #json으로 변환하기 위해 ''를 ""로 변환
    data = decrypt_data(data)
    data = str_to_json(data)

    #data 문자열 압축
    data['user_id'] = convert_uuid(data['user_id'])
    data['inDate'] = convert_inDate(data['inDate'])
    data['method'] = method_mapping(data['method'])
    data['url'] = url_mapping(data['url'])

    return data

#데이터 암호화
def encrypt_data(data):
    from cryptography.fernet import Fernet
    
    key = b't-jdqnDewRx9kWithdsTMS21eLrri70TpkMq2A59jX8='
    fernet = Fernet(key)

    tmp = fernet.encrypt(f"{data}".encode('ascii'))
    data = tmp.decode('ascii')

    return data

#데이터 압축
def gzip_compression(file_name, data):
    with gzip.open(file_name, 'wb') as f:
        f.write(f"{data}".encode('utf-8'))

#데이터 압축 해제
def gzip_decompression(file_name):
    with gzip.open(file_name, 'rb') as f:
        read_data = f.read().decode('utf-8').replace("'","\"")
    return read_data        

#중간에 inDate 값의 시간이 넘어갈 경우 데이터를 나눠서 압축
# ex) 100개의 데이터 중 첫 데이터의 inDate 값이 202303171420인데 80번째 데이터가 202303171520인 경우 시간이 14시에서 15시로 넘어간 데이터를 나눠서 압축
def hour_gzip_compression(file_name:list, result):
    for i in range(len(result)):
        gzip_compression(f'{file_name[i]}.gz', result[i])

#inDate 시간별 데이터 나누기
def data_hour_split(data):
    first_hour = data[0]['data']['inDate'][6:8]
    date_split = [0]

    for i in range(len(data)):
        next_hour = data[i]['data']['inDate'][6:8]
        if first_hour != next_hour:
            first_hour = next_hour
            date_split.append(i)
    date_split.append(100)

    result = []
    for i in range(len(date_split)-1):
        result.append(data[date_split[i]:date_split[i+1]])

    return result

#s3 연결
def connection_s3():
        client = boto3.client('s3',
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name = 'ap-northeast-2'
                )
        return client

#s3 데이터 업로드
def upload(client, date):
    #적재 파일 경로 data/year/month/data/hour를 만들기 위해 선언
    year = date[:4]
    month = date[4:6]
    day = date[6:8]
    hour = date[8:10]

    #파일명
    file_name= f"{date}.gz"

    path = f"data/{year}/{month}/{day}/{hour}/{file_name}"

    #upload_file(로컬원본경로,버킷이름,목적지경로)
    client.upload_file(f'{file_name}', os.getenv("BUCKET_NAME"), path)

def ETL_PIPELINE():
    print(f"ETL_PIPELINE START TIME : {datetime.datetime.now()}")
    url = "http://ec2-3-37-12-122.ap-northeast-2.compute.amazonaws.com/api/data/log"
    data = request_url(url)

    #데이터 복호화
    for i in range(len(data)):
        data[i]['data'] = decrypt(data[i]['data'])

    #시간별 데이터 나누기
    split_data = data_hour_split(data)

    #데이터 이름을 날짜로 변환 ex)202303171408.gz
    date_list = ["20" + i[-1]['data']['inDate'] for i in split_data]

    #데이터 암호화
    result = []
    for i in split_data:
        tmp = []
        for j in i:
            j['data'] = encrypt_data(j['data'])
            tmp.append(j)
        result.append(tmp)

    # 데이터 압축
    hour_gzip_compression(date_list, result)

    # aws s3 연결
    client = connection_s3()

    #데이터 업로드
    for i in date_list:
        upload(client, i)
        
    print(f"ETL_PIPELINE END TIME : {datetime.datetime.now()}")

# #main
if __name__=="__main__":
    scheduler = BlockingScheduler()
    
    scheduler.add_job(ETL_PIPELINE, 'interval', seconds = 300)

    scheduler.start()