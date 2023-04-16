#[data][user_id] -> b64uuid로 변환
def convert_uuid(id):
        import b64uuid
        
        return b64uuid.B64UUID(id[:32]).string + b64uuid.B64UUID(id[32:]).string

#원래의 user_id로 변환
def uuid_restore(id):
    import b64uuid

    return b64uuid.B64UUID(id[:22]).uuid.hex + b64uuid.B64UUID(id[22:]).uuid.hex