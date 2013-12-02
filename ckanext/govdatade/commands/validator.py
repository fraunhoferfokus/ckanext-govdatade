#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ckan import model
from ckan.lib.cli import CkanCommand
from ckan.logic import get_action
from ckanext.govdatade.util import normalize_action_dataset
from jinja2 import Environment, FileSystemLoader
from jsonschema.validators import Draft3Validator
from math import ceil

import ckanclient
import json
import urllib2


class Validator(CkanCommand):
    '''Validates datasets against the GovData.de JSON schema'''

    summary = __doc__.split('\n')[0]

    SCHEMA_URL = 'https://raw.github.com/fraunhoferfokus/ogd-metadata/master/OGPD_JSON_Schema.json'  # NOQA

    def __init__(self, name):
        super(Validator, self).__init__(name)
        self.schema = json.loads(urllib2.urlopen(self.SCHEMA_URL).read())

    def get_dataset_count(self, ckan):
        print 'Retrieve total number of datasets'
        return ckan.action('package_search', rows=1)['count']

    def get_datasets(self, ckan, rows, i):
        datasets = (i * 1000) + 1
        print 'Retrieve datasets %s - %s' % (datasets, datasets + rows - 1)

        records = ckan.action('package_search', rows=rows, start=rows * i)
        return records['results']

    def render_template(self, data):
        environment = Environment(loader=FileSystemLoader('lib/templates'))
        template = environment.get_template('validation.html.jinja2')
        return template.render(data)

    def write_validation_result(self, rendered_template):
        fd = open('output/validation.html', 'w')
        fd.write(rendered_template)
        fd.close()

    def validate_datasets(self, datasets, data):
        invalid_packages = 0
        broken_rules_count = 0
        broken_rules = {}

        print 'Validate datasets'
        for i, dataset in enumerate(datasets):
            identifier = dataset['id']

            broken_rules[identifier] = {}

            normalize_action_dataset(dataset)

            extras = dataset['extras']
            portal = extras.get('metadata_original_portal', None)

            data_providers = data['data_provider']
            data_providers[portal] = data_providers.get(portal, 0)

            errors = Draft3Validator(self.schema).iter_errors(dataset)

            if not Draft3Validator(self.schema).is_valid(dataset):
                data['broken_rules'][identifier] = []

                invalid_packages += 1
                data_providers[portal] += 1

                errors = Draft3Validator(self.schema).iter_errors(dataset)

                for error in errors:
                    broken_rules_count += 1
                    msg = error.message

                    path = [e for e in error.path if isinstance(e, basestring)]
                    path = str(' -> '.join(map((lambda e: str(e)), path)))
                    data['paths'][path] = data['paths'].get(path, 0) + 1
                    data['broken_rules'][identifier].append([path, msg])

                    path = str(list(error.path))
                    broken_rules[identifier][path] = msg

        return invalid_packages, broken_rules_count, broken_rules

    def command(self):

        if len(self.args) == 0:
            context = {'model':       model,
                       'session':     model.Session,
                       'ignore_auth': True}

            get_action('package_list')(context, {})
        else:
            endpoint = self.args[0]
            ckan = ckanclient.CkanClient(base_location=endpoint)

            ds = 0
            rs = 0
            br = {}

            data = {'paths': {}, 'data_provider': {}, 'broken_rules': {}}

            rows = 1000
            total = self.get_dataset_count(ckan)
            steps = int(ceil(total / float(rows)))

            for i in range(0, steps):
                if i == steps - 1:
                    rows = total - (i * rows)

                datasets = self.get_datasets(ckan, rows, i)
                d, r, b = self.validate_datasets(datasets, data)
                ds += d
                rs += r
                br = dict(br.items() + b.items())

            data['number_total_datasets'] = total
            data['number_valid_datasets'] = total - ds
            self.write_validation_result(self.render_template(data))
