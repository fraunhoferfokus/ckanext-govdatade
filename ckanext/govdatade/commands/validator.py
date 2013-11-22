#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ckan import model
from ckan.lib.cli import CkanCommand
from ckan.logic import get_action
from ckanext.govdatade.util import normalize_action_dataset
from jsonschema.validators import Draft3Validator

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

    def command(self):

        if len(self.args) == 0:
            context = {'model':       model,
                       'session':     model.Session,
                       'ignore_auth': True}

            # get_action('package_list')(context, {})
        else:
            endpoint = self.args[0]
            ckan = ckanclient.CkanClient(base_location=endpoint)

            packages = []
            count = 0
            while True:
                print 'Retreive the %sth batch' % (count + 1000)

                response = ckan.action('package_search', rows=1000)
                packages += response['results']

                count += 1000
                if count > response['count']:
                    break

            invalid_packages = 0
            broken_rules = 0

            for i, package in enumerate(packages):
                print 'Processing %s of %s' % (i, len(packages))

                normalize_action_dataset(package)
                errors = Draft3Validator(self.schema).iter_errors(package)

                if not Draft3Validator(self.schema).is_valid(package):
                    invalid_packages += 1
                    errors = Draft3Validator(self.schema).iter_errors(package)
                    for error in errors:
                        broken_rules += 1
                        print "%s -> %s" % (list(error.path), error.message)

            print 'Broken Rules: %s' % broken_rules
            print 'Invalid Packages: %s' % invalid_packages
