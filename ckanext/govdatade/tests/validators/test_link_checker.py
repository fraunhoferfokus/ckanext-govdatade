from ckanext.govdatade.validators.link_checker import LinkChecker

import datetime
import httpretty


class TestLinkChecker:

    def setUp(self):
        self.link_checker = LinkChecker()

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

        self.link_checker.record_failure(dataset_id, url, status)
        actual_record = eval(self.link_checker.redis_client.get(dataset_id))
        now = datetime.datetime.now()

        date_now = now.strftime("%Y-%m-%d")
        expected_record = {'id':    dataset_id,
                           'urls':  [{'url':     url,
                                      'status':  404,
                                      'date':    date_now,
                                      'strikes': 1}]}

        assert actual_record == expected_record
