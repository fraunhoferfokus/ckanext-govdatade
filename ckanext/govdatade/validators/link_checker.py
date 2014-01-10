from datetime import datetime

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

    def process_record(self, dataset):
        dataset_id = dataset['id']
        delete = False

        for resource in dataset['resources']:
            url = resource['url']

            try:
                code = self.validate(resource['url'])
                if self.is_available(code):
                    self.record_success(dataset_id, url)
                else:
                    delete = self.record_failure(dataset_id, url, code)
            except requests.Timeout:
                delete = self.record_failure(dataset_id, url, 'Timeout')

        return delete

    def check_dataset(self, dataset):
        results = []
        for resource in dataset['resources']:
            results.append(self.validate(resource['url']))
        return results

    def validate(self, url):
        response = requests.head(url, allow_redirects=True, timeout=3.0)
        return response.status_code

    def is_available(self, response_code):
        return response_code >= 200 and response_code < 300

    def record_failure(self, dataset_id, url, status, date=datetime.now()):
        delete = False
        record = eval(unicode(self.redis_client.get(dataset_id)))

        initial_url_record = {'status':  status,
                              'date':    date.strftime("%Y-%m-%d"),
                              'strikes': 1}

        # Record is not known yet
        if record is None:
            record = {'id':   dataset_id, 'urls': {}}
            record['urls'][url] = initial_url_record
            self.redis_client.set(dataset_id, record)

        # Record is known, but not that particular URL (Resource)
        elif url not in record['urls']:
            record['urls'][url] = initial_url_record
            self.redis_client.set(dataset_id, record)

        # Record and URL are known, increment Strike counter if 1+ day(s) have
        # passed since the last check
        else:
            url = record['urls'][url]
            last_updated = datetime.strptime(url['date'], "%Y-%m-%d")

            if last_updated < date:
                url['strikes'] += 1
                url['date'] = date.strftime("%Y-%m-%d")
                self.redis_client.set(dataset_id, record)

                if url['strikes'] >= 3:
                    delete = True

        return delete

    def record_success(self, dataset_id, url):
        record = self.redis_client.get(dataset_id)
        if record is not None:
            record = eval(record)

            # Remove URL entry due to working URL
            del record['urls'][url]

            # Remove record entry altogether if there are no failures anymore
            if not record['urls']:
                self.redis_client.delete(dataset_id)
            else:
                self.redis_client.set(dataset_id, record)
