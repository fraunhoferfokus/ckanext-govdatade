class LinkChecker:
    def __init__(self):
        pass

    def is_available(self, response):
        response_code = response[0]
        return response_code >= 200 and response_code < 300
