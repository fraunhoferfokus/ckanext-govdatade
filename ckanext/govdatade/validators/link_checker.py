import datetime
import redis
import requests


class LinkChecker:

    HEADERS = {'User-Agent': 'curl/7.29.0'}

    def __init__(self, db='production'):
        redis_db_dict = {'production': 0, 'test': 1}
        database = redis_db_dict[db]
        self.redis_client = redis.StrictRedis(host='localhost',
                                              port=6379,
                                              db=database)

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

    def record_failure(self, dataset_id, url, status):
        now = datetime.datetime.now()
        record = self.redis_client.get(dataset_id)

        if record is None:
            record = {'id':    dataset_id,
                      'urls':  [{'url':     url,
                                 'status':  status,
                                 'date':    now.strftime("%Y-%m-%d"),
                                 'strikes': 1}]}
            self.redis_client.set(dataset_id, record)
