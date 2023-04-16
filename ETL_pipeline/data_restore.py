import os
from dotenv import load_dotenv
load_dotenv("./env/key.env")

import modules.b64uuid_ as b64
import modules.transform as restore
import modules.cryptography_ as crypto
import modules.gzip_ as gzip_
import modules.aws_ as aws

#데이터 복호화
def decrypt(data):
    #decrypt시 bytes 객체로 리턴되어 decode
    #파이썬에서는 문자열을 ''로도 사용할 수 있지만 json 표준에서는 ""만 허용하므로 변환하기 위해 ''를 ""로 변환
    data = crypto.decrypt_data(data)
    data = restore.str_to_json(data)

    #압축한 문자열 되돌리기
    data['user_id'] = b64.uuid_restore(data['user_id'])
    data['inDate'] = restore.inDate_restore(data['inDate']) # datetime 변환 함수는 있으나 변환하지 않음
    data['method'] = restore.method_restore(data['method'])
    data['url'] = restore.url_restore(data['url'])

    return data

#데이터 복구
def data_restore(file_name):
    key = [os.getenv("AWS_ACCESS_KEY"), os.getenv("AWS_SECRET_ACCESS_KEY")]

    client = aws.connection_s3(key)
    aws.download(client, file_name, os.getenv("BUCKET_NAME"))
    
    data = gzip_.gzip_decompression("./download_file/"+file_name+".gz")
    data = restore.str_to_json(data)

    for i in data:
        i['data'] = decrypt(i['data'])

    return data

if __name__=="__main__":
    import sys
    # 파일 확장자는 입력하지 않아도 됨
    print(data_restore(sys.argv[1]))