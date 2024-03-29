import sys, os

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator

def extract():
    """
    1. 지정 url에서 데이터 추출하여 json으로 변환 request.request_url 함수
    2. 데이터를 json 파일로 저장
    3. json 파일명 = 데이터의 제일 앞과 뒤의 timestamp를 datetime으로 변환
    3. xcom을 이용하여 저장한 데이터 이름을 다음 task로 전송
    """
    import modules.requests_ as request

    url = "http://ec2-3-37-12-122.ap-northeast-2.compute.amazonaws.com/api/data/log"
    data = request.request_url(url)

    #파일명 생성 및 json 저장
    import modules.transform as tf
    import modules.json_ as json_

    file_name = tf.timestamp_to_datetime(data[0]['ArrivalTimeStamp']) +"-"+ tf.timestamp_to_datetime(data[-1]['ArrivalTimeStamp'])
    json_.save_json(file_name, data)
    print("file_name = : ", file_name)

    return file_name

#데이터 복호화
def decrypt(data):
    import modules.cryptography_ as crypto
    import modules.b64uuid_ as b64
    import modules.transform as transform_
    #decrypt시 bytes 객체로 리턴되어 decode
    #파이썬에서는 문자열을 ''로도 사용할 수 있지만 json 표준에서는 ""만 허용하므로 변환하기 위해 ''를 ""로 변환
    data = crypto.decrypt_data(data)
    data = transform_.str_to_json(data)

    #data 문자열 압축
    data['user_id'] = b64.convert_uuid(data['user_id'])
    data['inDate'] = transform_.convert_inDate(data['inDate'])
    data['method'] = transform_.method_mapping(data['method'])
    data['url'] = transform_.url_mapping(data['url'])

    return data

def transform(**context):
    """
    1. xcom을 이용하여 파일 이름을 가져온다.
    2. 가져온 데이터를 json 객체로 변환
    3. json 데이터를 복호화하고 로그 생성 시간 별로 데이터를 분리
    4. 분리된 데이터를 따로 암호화하여 압축
    5. xcom을 이용하여 파일 이름을 list에 넣어 전송
    """

    #------------생성한 모듈--------------
    import modules.transform as transform_
    import modules.cryptography_ as crypto
    import modules.gzip_ as gzip_
    import modules.json_ as json_
    #------------------------------------
    print("transform start")

    task_instance = context["task_instance"]
    file_name = task_instance.xcom_pull(task_ids = "extract_test")

    data = json_.load_json(file_name)

    #데이터 복호화
    for i in range(len(data)):
        data[i]['data'] = decrypt(data[i]['data'])

    #시간별 데이터 나누기
    split_data = transform_.data_hour_split(data)

    #데이터 이름을 날짜로 변환 ex)202303171408.gz
    date_list = ["20" + i[-1]['data']['inDate'] for i in split_data]


    #데이터 암호화
    result = []
    for i in split_data:
        tmp = []
        for j in i:
            j['data'] = crypto.encrypt_data(j['data'])
            tmp.append(j)
        result.append(tmp)

        # 데이터 압축
    gzip_.hour_gzip_compression(date_list, result)

    task_instance = context['task_instance']
    task_instance.xcom_push(key='date_list', value = date_list)

def load(**context):
    """
    1. xcom에서 압축파일명을 가져온다.
    2. aws s3에 연결하고 압축된 데이터들을 업로드
    """
    from dotenv import load_dotenv
    load_dotenv("../ETL_Pipeline/env/key.env")

    import modules.aws_ as aws

    key = [os.getenv("AWS_ACCESS_KEY"), os.getenv("AWS_SECRET_ACCESS_KEY")]

    # aws s3 연결
    client = aws.connection_s3(key)

    task_instance = context['task_instance']
    date_list = task_instance.xcom_pull(key='date_list')
    print("date_list = ", date_list)

    #데이터 업로드
    for i in date_list:
        print("s3 upload : ", i+".gz file")
        aws.upload(client, i, os.getenv("BUCKET_NAME"))

        
with DAG(
    "test_etl_pipeline",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    description="python etl_pipeline DAG",
    schedule=timedelta(minutes=5),
    start_date=datetime(2023, 3, 29),
    catchup=False,
    tags=["etl_pipeline"]
) as dag:
    t1 = PythonOperator(
        task_id="extract_test",
        python_callable = extract
    )

    t2 = PythonOperator(
        task_id="transform_test",
        depends_on_past=False,
        python_callable= transform,
        retries=3
    )

    t3 = PythonOperator(
        task_id="load_test",
        depends_on_past=False,
        python_callable=load
    )

    t1 >> t2 >> t3
