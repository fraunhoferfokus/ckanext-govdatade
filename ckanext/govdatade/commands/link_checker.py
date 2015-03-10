#!/usr/bin/env python
# -*- coding: utf8 -*-
from ckan import model
from ckan.model import Session
from ckan.lib.cli import CkanCommand
from ckan.logic import get_action, NotFound
from ckan.logic.schema import default_package_schema
from ckanext.govdatade.util import normalize_action_dataset

from ckan.lib.cli import CkanCommand
from ckanext.govdatade import CONFIG
from ckanext.govdatade.util import iterate_remote_datasets
from ckanext.govdatade.util import generate_link_checker_data
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
        data = {}
        generate_link_checker_data(data)
        self.write_report(self.render_template(data))

    def render_template(self, data):
        template_file = 'linkchecker-report.html.jinja2'

        template_dir = os.path.dirname(__file__)
        template_dir = os.path.join(template_dir, '../../..', 'lib/templates')
        template_dir = os.path.abspath(template_dir)

        environment = Environment(loader=FileSystemLoader(template_dir))
        template = environment.get_template(template_file)
        return template.render(data)

    def write_report(self, rendered_template):
        target_dir = CONFIG.get('validators', 'report_dir')
        target_dir = os.path.abspath(target_dir)
        output = os.path.join(target_dir, 'linkchecker.html')

        fd = open(output, 'w')
        fd.write(rendered_template.encode('UTF-8'))
        fd.close()

    def create_context(self):
        return {'model':       model,
                'session':     Session,
                'user':        u'harvest',
                'schema':      default_package_schema,
                'validate':    False}

    def command(self):
        super(LinkChecker,self)._load_config()
        context = self.create_context()
        if len(self.args) > 0:
            subcommand = self.args[0]
            if subcommand == 'remote':
                self.check_remote_host(self.args[1])
            elif subcommand == 'report':
                self.generate_report()         
            elif len(self.args) == 2 and self.args[0] == 'specific':
                dataset_name = self.args[1] 

                context = {'model':       model,
                           'session':     model.Session,
                           'ignore_auth': True}
            
                package_show = get_action('package_show')
                validator = link_checker.LinkChecker()

                dataset =  package_show(context, {'id': dataset_name})
                   
                print 'Processing dataset %s' % dataset
                normalize_action_dataset(dataset)
                validator.process_record(dataset)

