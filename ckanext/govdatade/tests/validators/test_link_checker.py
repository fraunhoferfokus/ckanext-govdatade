from ckanext.govdatade.validators.link_checker import LinkChecker

import httpretty


class TestLinkChecker:

    def setUp(self):
        self.link_checker = LinkChecker()

    def test_is_available(self):
        response = [200, "OK"]
        assert self.link_checker.is_available(response)

    @httpretty.activate
    def test_check_url(self):
        url = "http://example.com/dataset/1"
        httpretty.register_uri(httpretty.GET, url, status=200)
        self.link_checker.is_available([200, "OK"])
