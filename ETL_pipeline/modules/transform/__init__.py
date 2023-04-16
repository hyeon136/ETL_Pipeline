#------------transform-----------
#json의 ['data']['method'] 매핑
def method_mapping(data):
    mapping_dict = {"GET" : 1, "POST" : 2}
    
    return mapping_dict.get(data, 0)

#json의 ['data']['url'] 매핑
def url_mapping(data):
    mapping_dict = {"api" : 0, "users" : 1, 
                    "products" : 2, "data" : 3, 
                    "product" : 4, "log" : 5}
    
    return "/".join([str(mapping_dict[i]) for i in data.split("/") if i])

#날짜 형식 숫자만 남김
def convert_inDate(date:str):
    import re
    
    return re.sub("[^0-9]", "", date[2:])

def str_to_json(str):
    import json

    return json.loads(str.replace("'","\""))

#inDate 시간별 데이터 나누기
#데이터에서 inDate가 변경되는 부분이 있을 경우 
# date_split에 해당 위치를 저장하고 데이터를 나눠서 리턴
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


#-------------restore--------------------

#json의 ['data']['method'] 매핑
def method_restore(data):
    mapping_dict = {1 : "GET", 2 : "POST"}
    
    return mapping_dict[data]

#json의 ['data']['url'] 매핑
def url_restore(data):
    mapping_dict = {0 : "api", 1 : "users", 2 : "products", 3 : "data", 4 : "product", 5 : "log"}

    return "/".join([mapping_dict[int(i)] for i in data.split("/") if i])

def inDate_restore(date:str):
    return "20"+date[:2] + "-" + date[2:4] + "-" + date[4:6] + "T" + date[6:8] + ":" + date[8:10] + ":" + date[10:12] + "." + date[12:] + "Z"

#문자열을 datetime으로 변환
def str_to_datetime(string):
    import datetime

    return datetime.datetime.strptime(string, '%Y-%m-%dT%H:%M:%S.%fZ')

def timestamp_to_datetime(timestamp):
    import re
    from datetime import datetime
    
    timestamp = str(datetime.fromtimestamp(timestamp))

    time = re.sub("[^0-9]", "", timestamp)

    return time[2:12]