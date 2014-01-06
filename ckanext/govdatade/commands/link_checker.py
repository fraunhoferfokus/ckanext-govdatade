#!/usr/bin/env python
# -*- coding: utf8 -*-

from ckan.lib.cli import CkanCommand
from ckanext.govdatade.util import iterate_remote_datasets
from ckanext.govdatade.validators import link_checker


class LinkChecker(CkanCommand):
    '''Checks the availability of the dataset's URLs'''

    summary = __doc__.split('\n')[0]

    def __init__(self, name):
        super(LinkChecker, self).__init__(name)

    def command(self):

        if len(self.args) > 0:
            endpoint = self.args[0]
            checker = link_checker.LinkChecker()

            num_urls = 0
            num_success = 0
            for i, dataset in enumerate(iterate_remote_datasets(endpoint)):
                for resource in dataset['resources']:
                    num_urls += 1
                    url = resource['url'].encode('utf-8')
                    response_code = checker.validate(url)

                    if checker.is_available(response_code):
                        num_success += 1

            print num_urls
            print num_success
