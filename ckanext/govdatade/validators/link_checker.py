from datetime import datetime

import redis
import requests
import socket
import logging
import urllib2

log = logging.getLogger(__name__)

def logme(logText):      
        f = open('/opt/linkChecker.log','a')
        f.write(logText + '\n')
        f.close

class LinkChecker:

    HEADERS = {'User-Agent': 'curl/7.29.0'}
    TIMEOUT = 30.0

    def __init__(self, db='production'):
        redis_db_dict = {'production': 0, 'test': 1}
        database = redis_db_dict[db]
        self.redis_client = redis.StrictRedis(host='localhost',
                                              port=6379,
                                              db=database)

    def process_record(self, dataset):
        dataset_id = dataset['id']
	#logme("DATASET_ID: " + dataset_id)
        delete = False
        
        portal = None
        if 'extras' in dataset and \
           'metadata_original_portal' in dataset['extras']:
            portal = dataset['extras']['metadata_original_portal']
        for resource in dataset['resources']:
	    #logme("RESOURCE_URL: " + resource['url'])
            url = resource['url']
            url = url.replace('sequenz=tabelleErgebnis','sequenz=tabellen')
            url = url.replace('sequenz=tabelleDownload','sequenz=tabellen')
	    #logme("URL :" + url)
            try:
                code = self.validate(url)
		#logme("CODE: " + str(code))
                if self.is_available(code):
                    self.record_success(dataset_id, url)
                else:
                    delete = delete or self.record_failure(dataset, url,
                                                           code, portal)
            except requests.exceptions.Timeout:
                delete = delete or self.record_failure(dataset, url,
                                                       'Timeout', portal)
            except requests.exceptions.TooManyRedirects:
                delete = delete or self.record_failure(dataset, url,
                                                       'Redirect Loop', portal)
            except requests.exceptions.RequestException as e:
                if e is None:
                    delete = delete or self.record_failure(dataset, url,
                                                           'Unknown', portal)
                else:
                    delete = delete or self.record_failure(dataset, url, str(e), portal)
            except socket.timeout:
                delete = delete or self.record_failure(dataset, url,
                                                       'Timeout', portal)
        return delete

    def check_dataset(self, dataset):
        results = []
        for resource in dataset['resources']:
	    #logme("check_dataset: RESOURCE: " + resource['url'])
            #fca results.append(self.validate(resource['url']))
	    url = resource['url']
            url = url.replace('sequenz=tabelleErgebnis','sequenz=tabellen')
            url = url.replace('sequenz=tabelleDownload','sequenz=tabellen')
            results.append(self.validate(url))
	    #logme("check_dataset: RESOURCE: " + resource['url'])
	    #logme("check_dataset: RESULTS: " + results) 
        return results

    def validate(self, url):
	#logme(url)
	# do not check datasets from saxony until fix
	if "statistik.sachsen" in url:
	    return 200
	elif "www.bundesjustizamt.de" in url:
	    headers = { 'User-Agent' : 'Mozilla/5.0' }
	    req = urllib2.Request(url, None, headers)
	    try:
		respo = urllib2.urlopen(req)
		#logme("HTTP-CODE: " + str(respo.code))
		return respo.code
	    except urllib2.URLError, e:
		#logme("HTTP-CODE(e): " + str(e.code))
		return e.code
	else:
            response = requests.head(url, allow_redirects=True,timeout=self.TIMEOUT)
            if self.is_available(response.status_code):
                return response.status_code
	        #logme("validate_if: RESPONSE.status: " + response.status_code)
            else:
                response = requests.get(url, allow_redirects=True,timeout=self.TIMEOUT)
                #logme("validate_else: RESPONSE.status: " + str(response.status_code))
                return response.status_code

    def is_available(self, response_code):
        return response_code >= 200 and response_code < 300

    def record_failure(self, dataset, url, status, portal,
                       date=datetime.now().date()):
	#logme("record_failure: URL: " + url)
        dataset_id = dataset['id']
	#logme("record_failure: DATASET_ID: " + dataset_id)
        dataset_name = dataset['name']
        delete = False
        log.debug(self.redis_client.get(dataset_id))
        record = unicode(self.redis_client.get(dataset_id))
        try:     
           record = eval(record)
        except:
               print "Record_error: ", record    
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

        delete = record['urls'][url]['strikes'] >= 100

        return delete

    def record_success(self, dataset_id, url):
        record = self.redis_client.get(dataset_id)
        if record is not None:
            try:
               record = eval(unicode(record))
            except:
               print "ConnError"
            _type= type(record) is dict
            # Remove URL entry due to working URL
            if record.get('urls'):
               record['urls'].pop(url, None)
            # Remove record entry altogether if there are no failures
            # anymore
            if not record.get('urls'):
                self.redis_client.delete(dataset_id)
            else:
                self.redis_client.set(dataset_id, record)

    def get_records(self):
        result = []
        for dataset_id in self.redis_client.keys('*'):
            #print "DS_id: ",dataset_id
	    if dataset_id == 'general':
                continue
	    try:
                result.append(eval(self.redis_client.get(dataset_id)))
	    except:
		print "DS_error: ", dataset_id

        return result
