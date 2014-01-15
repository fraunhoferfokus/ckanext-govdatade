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

    def check_remote_host(self, endpoint):
        checker = link_checker.LinkChecker()

        num_urls = 0
        num_success = 0
        for i, dataset in enumerate(iterate_remote_datasets(endpoint)):
            print 'Process %s' % i
            for resource in dataset['resources']:
                num_urls += 1
                url = resource['url'].encode('utf-8')
                response_code = checker.validate(url)
                print response_code

                if checker.is_available(response_code):
                    num_success += 1

        print num_urls
        print num_success

    def generate_report(self):
        checker = link_checker.LinkChecker()
        for record in checker.get_records():
            for url, info in record['urls'].items():
                print url
                print info

    def command(self):

        if len(self.args) > 0:
            subcommand = self.args[0]
            if subcommand == 'remote':
                self.check_remote_host(self.args[1])
            elif subcommand == 'report':
                self.generate_report()
