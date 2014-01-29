from datetime import datetime

import redis
import requests
import socket


class LinkChecker:

    HEADERS = {'User-Agent': 'curl/7.29.0'}
    TIMEOUT = 10.0

    def __init__(self, db='production'):
        redis_db_dict = {'production': 0, 'test': 1}
        database = redis_db_dict[db]
        self.redis_client = redis.StrictRedis(host='localhost',
                                              port=6379,
                                              db=database)

    def process_record(self, dataset):
        dataset_id = dataset['id']
        delete = False
        portal = None

        if 'extras' in dataset and \
           'metadata_original_portal' in dataset['extras']:
            portal = dataset['extras']['metadata_original_portal']

        for resource in dataset['resources']:
            url = resource['url']

            try:
                code = self.validate(resource['url'])
                if self.is_available(code):
                    self.record_success(dataset_id, url)
                else:
                    delete = delete or self.record_failure(dataset, url,
                                                           code, portal)
            except requests.exceptions.Timeout:
                delete = delete or self.record_failure(dataset_id, url,
                                                       'Timeout', portal)
            except requests.exceptions.TooManyRedirects:
                delete = delete or self.record_failure(dataset_id, url,
                                                       'Redirect Loop', portal)
            except requests.exceptions.RequestException as e:
                if e is None:
                    delete = delete or self.record_failure(dataset_id, url,
                                                           'Unknown')
                else:
                    delete = delete or self.record_failure(dataset_id, url, e)
            except socket.timeout:
                delete = delete or self.record_failure(dataset_id, url,
                                                       'Timeout', portal)
        return delete

    def check_dataset(self, dataset):
        results = []
        for resource in dataset['resources']:
            results.append(self.validate(resource['url']))
        return results

    def validate(self, url):
        response = requests.head(url, allow_redirects=True,
                                 timeout=self.TIMEOUT)

        if self.is_available(response.status_code):
            return response.status_code
        else:
            response = requests.get(url, allow_redirects=True,
                                    timeout=self.TIMEOUT)
            return response.status_code

    def is_available(self, response_code):
        return response_code >= 200 and response_code < 300

    def record_failure(self, dataset, url, status, portal,
                       date=datetime.now().date()):

        dataset_id = dataset['id']
        dataset_name = dataset['name']

        delete = False
        record = eval(unicode(self.redis_client.get(dataset_id)))

        initial_url_record = {'status':  status,
                              'date':    date.strftime("%Y-%m-%d"),
                              'strikes': 1}

        if record is not None:
            record['name'] = dataset_name
            record['metadata_original_portal'] = portal
            self.redis_client.set(dataset_id, record)

        # Record is not known yet
        if record is None:
            record = {'id': dataset_id, 'name': dataset_name, 'urls': {}}
            record['urls'][url] = initial_url_record
            record['metadata_original_portal'] = portal
            self.redis_client.set(dataset_id, record)

        # Record is known, but not that particular URL (Resource)
        elif url not in record['urls']:
            record['urls'][url] = initial_url_record
            self.redis_client.set(dataset_id, record)

        # Record and URL are known, increment Strike counter if 1+ day(s) have
        # passed since the last check
        else:
            url_entry = record['urls'][url]
            last_updated = datetime.strptime(url_entry['date'], "%Y-%m-%d")
            last_updated = last_updated.date()

            if last_updated < date:
                url_entry['strikes'] += 1
                url_entry['date'] = date.strftime("%Y-%m-%d")
                self.redis_client.set(dataset_id, record)

        delete = record['urls'][url]['strikes'] >= 3

        return delete

    def record_success(self, dataset_id, url):
        record = self.redis_client.get(dataset_id)
        if record is not None:
            record = eval(record)

            # Remove URL entry due to working URL
            record['urls'].pop(url, None)

            # Remove record entry altogether if there are no failures
            # anymore
            if not record['urls']:
                self.redis_client.delete(dataset_id)
            else:
                self.redis_client.set(dataset_id, record)

    def get_records(self):
        result = []
        for dataset_id in self.redis_client.keys('*'):
            result.append(eval(self.redis_client.get(dataset_id)))

        return result
