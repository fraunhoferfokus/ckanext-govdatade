import requests


class LinkChecker:

    HEADERS = {'User-Agent': 'curl/7.29.0'}

    def check_dataset(self, dataset):
        results = []
        for resource in dataset['resources']:
            results.append(self.validate(resource['url']))
        return results

    def validate(self, url):
        response = requests.head(url, allow_redirects=True)
        return response.status_code

    def is_available(self, response_code):
        return response_code >= 200 and response_code < 300
