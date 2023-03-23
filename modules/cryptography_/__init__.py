#데이터 암호화
def encrypt_data(data):
    from cryptography.fernet import Fernet
    
    key = b't-jdqnDewRx9kWithdsTMS21eLrri70TpkMq2A59jX8='
    fernet = Fernet(key)

    tmp = fernet.encrypt(f"{data}".encode('ascii'))
    data = tmp.decode('ascii')

    return data

#데이터 복호화
def decrypt_data(data):
    from cryptography.fernet import Fernet
    
    key = b't-jdqnDewRx9kWithdsTMS21eLrri70TpkMq2A59jX8='
    fernet = Fernet(key)

    return fernet.decrypt(data).decode('ascii')