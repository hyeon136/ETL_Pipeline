def convert_uuid(id):
    import b64uuid
    return b64uuid.B64UUID(id[:32]).string + b64uuid.B64UUID(id[32:]).string

def url_mapping(data):
    mapping_dict = {"api" : 0, "users" : 1,
                    "products" : 2, "data" : 3,
                    "product" : 4, "log" : 5}

    return "/".join([str(mapping_dict[i]) for i in data.split("/") if i])

def decrypt_data(data):
    from cryptography.fernet import Fernet
    key = b't-jdqnDewRx9kWithdsTMS21eLrri70TpkMq2A59jX8='
    fernet = Fernet(key)
    return fernet.decrypt(data).decode('ascii')

def method_mapping(data):
    mapping_dict = {"GET" : 1, "POST" : 2}
    return mapping_dict.get(data, 0)

def convert_inDate(date:str):
    import re

    return re.sub("[^0-9]", "", date[2:19])
