from ckanext.govdatade.validators.link_checker import LinkChecker

import datetime
import httpretty
import unittest


class TestLinkChecker(unittest.TestCase):

    def setUp(self):
        self.link_checker = LinkChecker()
        self.link_checker.redis_client.flushdb()

    def tearDown(self):
        self.link_checker.redis_client.flushdb()

    def test_is_available_200(self):
        assert self.link_checker.is_available(200)

    def test_is_available_404(self):
        assert not self.link_checker.is_available(400)

    @httpretty.activate
    def test_check_url_200(self):
        url = 'http://example.com/dataset/1'
        httpretty.register_uri(httpretty.HEAD, url, status=200)

        expectation = 200
        assert self.link_checker.validate(url) == expectation

    @httpretty.activate
    def test_check_url_404(self):
        url = 'http://example.com/dataset/1'
        httpretty.register_uri(httpretty.HEAD, url, status=404)

        expectation = 404
        assert self.link_checker.validate(url) == expectation

    @httpretty.activate
    def test_check_url_301(self):
        url = 'http://example.com/dataset/1'
        target = 'http://www.example.com/dataset/1'

        httpretty.register_uri(httpretty.HEAD, target, status=200)
        httpretty.register_uri(httpretty.HEAD, url, status=301,
                               location=target)

        expectation = 200
        assert self.link_checker.validate(url) == expectation

    @httpretty.activate
    def test_check_dataset(self):
        url1 = 'http://example.com/dataset/1'
        httpretty.register_uri(httpretty.HEAD, url1, status=200)
        url2 = 'http://example.com/dataset/2'
        httpretty.register_uri(httpretty.HEAD, url2, status=404)
        url3 = 'http://example.com/dataset/3'
        httpretty.register_uri(httpretty.HEAD, url3, status=200)

        dataset = {'id': 1,
                   'resources': [{'url': url1}, {'url': url2}, {'url': url3}]}

        assert self.link_checker.check_dataset(dataset) == [200, 404, 200]

    def test_redis(self):
        assert self.link_checker.redis_client.ping()

    def test_record_failure(self):
        dataset_id = '1'
        url = 'https://www.example.com'
        status = 404
        portal = 'example.com'

        date = datetime.datetime(2014, 1, 1)
        self.link_checker.record_failure(dataset_id, url, status, portal, date)
        actual_record = eval(self.link_checker.redis_client.get(dataset_id))

        date_string = date.strftime("%Y-%m-%d")
        expected_record = {'id':    dataset_id,
                           'urls':  {url: {'status':  404,
                                           'date':    date_string,
                                           'strikes': 1}},
                           'metadata_original_portal': portal}

        assert actual_record == expected_record

    def test_record_failure_second_time_same_date(self):
        dataset_id = '1'
        url = 'https://www.example.com'
        status = 404

        date = datetime.datetime(2014, 1, 1)
        self.link_checker.record_failure(dataset_id, url, status, None, date)

        # Second time to test that the strikes counter has not incremented
        self.link_checker.record_failure(dataset_id, url, status, None, date)

        actual_record = eval(self.link_checker.redis_client.get(dataset_id))

        date_string = date.strftime("%Y-%m-%d")
        expected_record = {'id':    dataset_id,
                           'urls':  {url: {'status':  404,
                                           'date':    date_string,
                                           'strikes': 1}},
                           'metadata_original_portal': None}

        assert actual_record == expected_record

    def test_record_failure_second_time_different_date(self):
        dataset_id = '1'
        url = 'https://www.example.com'
        status = 404
        portal = 'example.com'

        date = datetime.datetime(2014, 1, 1)
        self.link_checker.record_failure(dataset_id, url, status, portal, date)

        date = datetime.datetime(2014, 1, 2)
        self.link_checker.record_failure(dataset_id, url, status, portal, date)

        actual_record = eval(self.link_checker.redis_client.get(dataset_id))

        date_string = date.strftime("%Y-%m-%d")
        expected_record = {'id':    dataset_id,
                           'urls':  {url: {'status':  404,
                                           'date':    date_string,
                                           'strikes': 2}},
                           'metadata_original_portal': portal}

        self.assertEqual(actual_record, expected_record)

    def test_record_success(self):
        dataset_id = '1'
        url = 'https://www.example.com'

        self.link_checker.record_success(dataset_id, url)

        entry = self.link_checker.redis_client.get(dataset_id)
        assert entry is None

    def test_record_success_after_failure(self):
        dataset_id = '1'
        url = 'https://www.example.com'
        status = 404
        portal = None

        date = datetime.datetime(2014, 1, 1)
        self.link_checker.record_failure(dataset_id, url, status, portal, date)
        actual_record = eval(self.link_checker.redis_client.get(dataset_id))

        date_string = date.strftime("%Y-%m-%d")
        expected_record = {'id':    dataset_id,
                           'urls':  {url: {'status':  404,
                                           'date':    date_string,
                                           'strikes': 1}},
                           'metadata_original_portal': None}

        self.assertEqual(actual_record, expected_record)

        self.link_checker.record_success(dataset_id, url)
        self.assertIsNone(self.link_checker.redis_client.get(dataset_id))

    def test_url_success_after_failure(self):
        dataset_id = '1'

        url1 = 'https://www.example.com/dataset/1'
        url2 = 'https://www.example.com/dataset/2'
        portal = 'example.com'

        date = datetime.datetime(2014, 1, 1)
        date_string = date.strftime("%Y-%m-%d")

        self.link_checker.record_failure(dataset_id, url1, 404, portal, date)
        self.link_checker.record_failure(dataset_id, url2, 404, portal, date)

        actual_record = eval(self.link_checker.redis_client.get(dataset_id))

        expected_record = {'id':    dataset_id,
                           'urls':  {url1: {'status':  404,
                                            'date':    date_string,
                                            'strikes': 1},
                                     url2: {'status':  404,
                                            'date':    date_string,
                                            'strikes': 1}},
                           'metadata_original_portal': portal}

        self.assertEqual(actual_record, expected_record)
        self.link_checker.record_success(dataset_id, url1)

        actual_record = eval(self.link_checker.redis_client.get(dataset_id))

        expected_record = {'id':    dataset_id,
                           'urls':  {url2: {'status':  404,
                                            'date':    date_string,
                                            'strikes': 1}},
                           'metadata_original_portal': portal}

        self.assertEqual(actual_record, expected_record)

    @httpretty.activate
    def test_process_record(self):
        url1 = 'http://example.com/dataset/1'
        url2 = 'http://example.com/dataset/2'

        httpretty.register_uri(httpretty.HEAD, url1, status=200)
        httpretty.register_uri(httpretty.HEAD, url2, status=404)

        dataset = {'id': 1, 'resources': [{'url': url1}, {'url': url2}]}

        self.link_checker.process_record(dataset)
        record = eval(self.link_checker.redis_client.get(1))

        self.assertNotIn(url1, record['urls'])
        self.assertEqual(record['urls'][url2]['strikes'], 1)
