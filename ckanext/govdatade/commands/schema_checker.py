#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ckan import model
from ckan.model import Session
from ckan.lib.cli import CkanCommand
from ckan.logic import get_action, NotFound
from ckan.logic.schema import default_package_schema

from ckanext.govdatade.config import CONFIG
from ckanext.govdatade.util import normalize_action_dataset
from ckanext.govdatade.util import iterate_local_datasets
from ckanext.govdatade.validators import schema_checker

from collections import defaultdict
from jinja2 import Environment, FileSystemLoader
from jsonschema.validators import Draft3Validator
from math import ceil

import ckanclient
import os


class SchemaChecker(CkanCommand):
    '''Validates datasets against the GovData.de JSON schema'''

    summary = __doc__.split('\n')[0]

    SCHEMA_URL = 'https://raw.github.com/fraunhoferfokus/ogd-metadata/master/OGPD_JSON_Schema.json'  # NOQA

    def __init__(self, name):
        super(SchemaChecker, self).__init__(name)

    def get_dataset_count(self, ckan):
        print 'Retrieve total number of datasets'
        return ckan.action('package_search', rows=1)['count']

    def get_datasets(self, ckan, rows, i):
        datasets = (i * 1000) + 1
        print 'Retrieve datasets %s - %s' % (datasets, datasets + rows - 1)

        records = ckan.action('package_search', rows=rows, start=rows * i)
        return records['results']

    def render_template(self, template_file, data):

        template_dir = os.path.dirname(__file__)
        template_dir = os.path.join(template_dir, '../../..', 'lib/templates')
        template_dir = os.path.abspath(template_dir)

        environment = Environment(loader=FileSystemLoader(template_dir))
        template = environment.get_template(template_file)
        return template.render(data)

    def write_validation_result(self, rendered_template, template_file):
        target_file = template_file.rstrip(".jinja2")

        target_dir = CONFIG.get('validators', 'report_dir')
        target_dir = os.path.join(target_dir, target_file)
        target_dir = os.path.abspath(target_dir)

        fd = open(target_dir, 'w')
        fd.write(rendered_template.encode('UTF-8'))
        fd.close()

    def validate_datasets(self, dataset, data):
        normalize_action_dataset(dataset)

        identifier = dataset['id']
        portal = dataset['extras'].get('metadata_original_portal', 'null')

        data['broken_rules'][portal][identifier] = []
        broken_rules = data['broken_rules'][portal][identifier]

        data['datasets_per_portal'][portal].add(identifier)
        errors = Draft3Validator(self.schema).iter_errors(dataset)

        if Draft3Validator(self.schema).is_valid(dataset):
            data['valid_datasets'] += 1
        else:
            data['invalid_datasets'] += 1
            errors = Draft3Validator(self.schema).iter_errors(dataset)

            for error in errors:
                path = [e for e in error.path if isinstance(e, basestring)]
                path = str(' -> '.join(map((lambda e: str(e)), path)))

                data['field_paths'][path] += 1
                field_path_message = [path, error.message]
                broken_rules.append(field_path_message)

    def create_context(self):
        return {'model':       model,
                'session':     Session,
                'user':        u'harvest',
                'schema':      default_package_schema,
                'validate':    False}

    def command(self):
        super(SchemaChecker, self)._load_config()
        context = self.create_context()

        data = {'field_paths':         defaultdict(int),
                'broken_rules':        defaultdict(dict),
                'datasets_per_portal': defaultdict(set),
                'invalid_datasets':    0,
                'valid_datasets':      0}

        if len(self.args) == 0:

            context = {'model':       model,
                       'session':     model.Session,
                       'ignore_auth': True}

            validator = schema_checker.SchemaChecker()

            num_datasets = 0
            for i, dataset in enumerate(iterate_local_datasets(context)):
                print 'Processing dataset %s' % i
                normalize_action_dataset(dataset)
                validator.process_record(dataset)
                num_datasets += 1

            general = {'num_datasets': num_datasets}
            validator.redis_client.set('general', general)

        elif len(self.args) == 2 and self.args[0] == 'specific':

            context = {'model':       model,
                       'session':     model.Session,
                       'ignore_auth': True}
            
            package_show = get_action('package_show')
            dataset_name = self.args[1]
            dataset = package_show(context, {'id': dataset_name})
                   
            print 'Processing dataset %s' % dataset
            normalize_action_dataset(dataset)
            validator = schema_checker.SchemaChecker()
            validator.process_record(dataset)

        elif len(self.args) == 2 and self.args[0] == 'remote':
            endpoint = self.args[1]
            ckan = ckanclient.CkanClient(base_location=endpoint)

            rows = 1000
            total = self.get_dataset_count(ckan)
            steps = int(ceil(total / float(rows)))

            for i in range(0, steps):
                if i == steps - 1:
                    rows = total - (i * rows)

                datasets = self.get_datasets(ckan, rows, i)
                self.validate_datasets(datasets, data)

            self.write_validation_result(self.render_template(data))
