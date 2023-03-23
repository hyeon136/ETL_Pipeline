#s3 연결
def connection_s3(key):
        import boto3
        client = boto3.client('s3',
                aws_access_key_id=key[0],
                aws_secret_access_key=key[1],
                region_name = 'ap-northeast-2'
                )
        return client

#s3 데이터 업로드
def upload(client, date, bucket_name):
    #적재 파일 경로 data/year/month/data/hour를 만들기 위해 선언
    year = date[:4]
    month = date[4:6]
    day = date[6:8]
    hour = date[8:10]

    #파일명
    file_name= f"{date}.gz"

    path = f"data/{year}/{month}/{day}/{hour}/{file_name}"

    #upload_file(로컬원본경로,버킷이름,목적지경로)
    client.upload_file(f'{file_name}', bucket_name, path)

#s3 데이터 다운로드
def download(client, file_name, bucket_name):
    year = file_name[:4]
    month = file_name[4:6]
    day = file_name[6:8]
    hour = file_name[8:10]

    #파일명
    file= f"{file_name}.gz"

    path = f"data/{year}/{month}/{day}/{hour}/{file}"
    
    client.download_file(bucket_name, path, f"./download_file/{file}")