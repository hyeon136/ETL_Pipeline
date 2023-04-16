import os
import sys
import datetime
from dotenv import load_dotenv
load_dotenv("./env/key.env")

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

#------------생성한 모듈--------------
import modules.transform as transform
import modules.cryptography_ as crypto
import modules.b64uuid_ as b64
import modules.gzip_ as gzip_
import modules.requests_ as request
import modules.aws_ as aws
#------------------------------------

#데이터 복호화
def decrypt(data):
    #decrypt시 bytes 객체로 리턴되어 decode
    #파이썬에서는 문자열을 ''로도 사용할 수 있지만 json 표준에서는 ""만 허용하므로 변환하기 위해 ''를 ""로 변환
    data = crypto.decrypt_data(data)
    data = transform.str_to_json(data)

    #data 문자열 압축
    data['user_id'] = b64.convert_uuid(data['user_id'])
    data['inDate'] = transform.convert_inDate(data['inDate'])
    data['method'] = transform.method_mapping(data['method'])
    data['url'] = transform.url_mapping(data['url'])

    return data

def etl_pipeline():
    print(f"ETL_PIPELINE START TIME : {datetime.datetime.now()}")
    url = "http://ec2-3-37-12-122.ap-northeast-2.compute.amazonaws.com/api/data/log"
    data = request.request_url(url)

    decrypt_data = []
    #데이터 복호화
    for i in data:
        decrypt_data.append(decrypt(i['data']))

    print(decrypt_data)
    #시간별 데이터 나누기
    split_data = transform.data_hour_split(data)

    #데이터 이름을 날짜로 변환 ex)202303171408.gz
    date_list = ["20" + i[-1]['data']['inDate'] for i in split_data]


    # #데이터 암호화
    # result = []
    # for i in split_data:
    #     tmp = []
    #     for j in i:
    #         j['data'] = crypto.encrypt_data(j['data'])
    #         tmp.append(j)
    #     result.append(tmp)

    # # 데이터 압축
    # gzip_.hour_gzip_compression(date_list, result)

    # key = [os.getenv("AWS_ACCESS_KEY"), os.getenv("AWS_SECRET_ACCESS_KEY")]

    # # aws s3 연결
    # client = aws.connection_s3(key)

    # #데이터 업로드
    # for i in date_list:
    #     aws.upload(client, i, os.getenv("BUCKET_NAME"))
        
    # print(f"ETL_PIPELINE END TIME : {datetime.datetime.now()}")

# #main
if __name__=="__main__":
    """스케줄링 코드 5분마다 실행"""
    # from apscheduler.schedulers.blocking import BlockingScheduler

    # scheduler = BlockingScheduler()
    
    # scheduler.add_job(etl_pipeline, 'interval', seconds=300)

    # scheduler.start()

    etl_pipeline()