def request_url(url):
    import requests
    
    try:
        print("data request function")
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print("Error occurred : ", e)
    else:
        return response.json()
    finally:
        print("data request function end")