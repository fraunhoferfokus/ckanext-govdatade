#!/usr/bin/env python
# -*- coding: utf8 -*-

from ckan.lib.cli import CkanCommand
from ckanext.govdatade import CONFIG
from ckanext.govdatade.util import iterate_remote_datasets
from ckanext.govdatade.validators import link_checker
from collections import defaultdict
from jinja2 import Environment, FileSystemLoader

import os
import requests


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
        url = 'https://www.govdata.de/ckan/api/action/package_search?q='
        num_metadata = requests.get(url).json()['result']['count']

        data = {'portals': defaultdict(int), 'entries': []}

        for record in checker.get_records():
            for url, entry in record['urls'].iteritems():
                if type(entry['status']) == int:
                    entry['status'] = 'HTTP %s' % entry['status']

            if 'metadata_original_portal' in record:  # legacy
                portal = record['metadata_original_portal']
                data['portals'][portal] += 1
                data['entries'].append(record)

        data['count'] = sum(data['portals'].values())
        data['working'] = num_metadata - data['count']

        self.write_report(self.render_template(data))

    def render_template(self, data):
        filename = 'templates/linkchecker-report.html.jinja2'
        environment = Environment(loader=FileSystemLoader('lib'))
        template = environment.get_template(filename)
        return template.render(data)

    def write_report(self, rendered_template):
        target_dir = CONFIG.get('validators', 'report_dir')
        target_dir = os.path.dirname(target_dir)
        output = os.path.join(target_dir, 'linkchecker.html')

        fd = open(output, 'w')
        fd.write(rendered_template.encode('UTF-8'))
        fd.close()

    def command(self):

        if len(self.args) > 0:
            subcommand = self.args[0]
            if subcommand == 'remote':
                self.check_remote_host(self.args[1])
            elif subcommand == 'report':
                self.generate_report()
