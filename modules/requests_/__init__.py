def request_url(url):
    import requests
    
    return requests.get(url).json()