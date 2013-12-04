#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ckan import model
from ckan.lib.cli import CkanCommand
from ckan.logic import get_action
from ckanext.govdatade.util import normalize_action_dataset
from collections import defaultdict
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
        environment = Environment(loader=FileSystemLoader('lib'))
        template = environment.get_template('templates/validation.html.jinja2')
        return template.render(data)

    def write_validation_result(self, rendered_template):
        fd = open('output/validation.html', 'w')
        fd.write(rendered_template)
        fd.close()

    def validate_datasets(self, datasets, data):

        print 'Validate datasets'
        for i, dataset in enumerate(datasets):
            normalize_action_dataset(dataset)

            identifier = dataset['id']
            portal = dataset['extras'].get('metadata_original_portal', 'null')
            portal = portal.replace('http://', '')
            portal = portal.replace('/', '')

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

    def command(self):

        if len(self.args) == 0:
            context = {'model':       model,
                       'session':     model.Session,
                       'ignore_auth': True}

            get_action('package_list')(context, {})
        else:
            endpoint = self.args[0]
            ckan = ckanclient.CkanClient(base_location=endpoint)

            data = {'field_paths':               defaultdict(int),
                    'broken_rules':              defaultdict(dict),
                    'datasets_per_portal':       defaultdict(set),
                    'invalid_datasets':          0,
                    'valid_datasets':            0}

            rows = 1000
            total = self.get_dataset_count(ckan)
            steps = int(ceil(total / float(rows)))

            for i in range(0, steps):
                if i == steps - 1:
                    rows = total - (i * rows)

                datasets = self.get_datasets(ckan, rows, i)
                self.validate_datasets(datasets, data)

            self.write_validation_result(self.render_template(data))
