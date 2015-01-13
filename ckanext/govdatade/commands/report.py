#!/usr/bin/env python
# -*- coding: utf8 -*-

from collections import defaultdict
from ckan.lib.cli import CkanCommand

from ckanext.govdatade import CONFIG
from ckanext.govdatade.util import copy_report_vendor_files
from ckanext.govdatade.util import copy_report_asset_files

from ckanext.govdatade.util import generate_link_checker_data
from ckanext.govdatade.util import generate_schema_checker_data
from ckanext.govdatade.util import generate_general_data
from ckanext.govdatade.util import amend_portal

from jinja2 import Environment, FileSystemLoader

import os


class Report(CkanCommand):
    '''Generates metadata quality report based on Redis data.'''

    summary = __doc__.split('\n')[0]

    def __init__(self, name):
        super(Report, self).__init__(name)

    def generate_report(self):
        data = defaultdict(defaultdict)

        generate_general_data(data)
        generate_link_checker_data(data)
        generate_schema_checker_data(data)

        copy_report_asset_files()
        copy_report_vendor_files()

        templates = ['index.html', 'linkchecker.html', 'schemachecker.html']
        templates = map(lambda name: name + '.jinja2', templates)
        for template_file in templates:
            rendered_template = self.render_template(template_file, data)
            self.write_validation_result(rendered_template, template_file)

    def render_template(self, template_file, data):

        template_dir = os.path.dirname(__file__)
        template_dir = os.path.join(template_dir, '../../..', 'lib/templates')
        template_dir = os.path.abspath(template_dir)

        environment = Environment(loader=FileSystemLoader(template_dir))
        environment.globals.update(amend_portal=amend_portal)

        template = environment.get_template(template_file)
        return template.render(data)

    def write_validation_result(self, rendered_template, template_file):
        target_file = template_file.rstrip(".jinja2")

        target_dir = "/var/lib/ckan/one/static/reports/" #CONFIG.get('validators', 'report_dir')
        target_dir = os.path.join(target_dir, target_file)
        target_dir = os.path.abspath(target_dir)

        fd = open(target_dir, 'w')
        fd.write(rendered_template.encode('UTF-8'))
        fd.close()

    def command(self):
        self.generate_report()
