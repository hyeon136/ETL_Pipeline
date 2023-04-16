#데이터 압축
def gzip_compression(file_name:str, data):
    import gzip

    print("데이터 압축 중......")

    with gzip.open(file_name, 'wb') as f:
        f.write(f"{data}".encode('utf-8'))

    print("데이터 압축완료!!")

#데이터 압축 해제
def gzip_decompression(file_name):
    import gzip
    
    print("데이터 압축 해제 중....")

    with gzip.open(file_name, 'rb') as f:
        read_data = f.read().decode('utf-8').replace("'","\"") # type: ignore

    print("데이터 압축 해제 완료")
    return read_data

#중간에 inDate 값의 시간이 넘어갈 경우 데이터를 나눠서 압축
# ex) 100개의 데이터 중 첫 데이터의 inDate 값이 202303171420인데 80번째 데이터가 202303171520인 경우 시간이 14시에서 15시로 넘어간 데이터를 나눠서 압축
def hour_gzip_compression(file_name:list, result):
    for i in range(len(result)):
        gzip_compression(f'{file_name[i]}.gz', result[i])