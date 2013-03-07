from ckan.lib.helpers import json
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.govdatade.harvesters.translator import translate_groups

import ConfigParser
import logging
import os
import urllib2


log = logging.getLogger(__name__)


def assert_author_fields(package_dict, author_alternative, author_email_alternative):
    """Ensures that the author field is set."""

    if not package_dict['author']:
        package_dict['author'] = author_alternative

    if not package_dict['author_email']:
        package_dict['author_email'] = author_email_alternative

    if not package_dict['author']:
        raise Exception('There is no author for package %s' % package_dict['id'])


class GroupCKANHarvester(CKANHarvester):
    """An extended CKAN harvester that also imports remote groups, for that api version 1 is enforced"""

    api_version = 1
    """Enforce API version 1 for enabling group import"""

    def _set_config(self, config_str):
        """Enforce API version 1 for enabling group import"""
        if config_str:
            self.config = json.loads(config_str)
        else:
            self.config = {}
        self.api_version = 1
        self.config['api_version'] = '1'


class HamburgCKANHarvester(GroupCKANHarvester):
    """A CKAN Harvester for Hamburg solving data compatibility problems."""

    def info(self):
        return {'name':        'hamburg',
                'title':       'Hamburg Harvester',
                'description': 'A CKAN Harvester for Hamburg solving data compatibility problems.'}

    def import_stage(self, harvest_object):
        package_dict = json.loads(harvest_object.content)

        package_dict['groups'] = [name.replace('-', '_') for name in package_dict['groups']]
        package_dict['tags'].append(u'Hamburg')
        assert_author_fields(package_dict, package_dict['maintainer'], package_dict['maintainer_email'])

        harvest_object.content = json.dumps(package_dict)
        super(HamburgCKANHarvester, self).import_stage(harvest_object)


class BerlinCKANHarvester(GroupCKANHarvester):
    """A CKAN Harvester for Berlin sovling data compatibility problems."""

    def info(self):
        return {'name':        'berlin',
                'title':       'Berlin Harvester',
                'description': 'A CKAN Harvester for Berlin solving data compatibility problems.'}

    def import_stage(self, harvest_object):
        package_dict = json.loads(harvest_object.content)

        if package_dict['license_id'] == '':
            package_dict['license_id'] = 'notspecified'

        package_dict['groups'] = translate_groups(package_dict['groups'], 'berlin')

        harvest_object.content = json.dumps(package_dict)
        super(BerlinCKANHarvester, self).import_stage(harvest_object)


class RLPCKANHarvester(GroupCKANHarvester):
    """A CKAN Harvester for Rhineland-Palatinate sovling data compatibility problems."""

    def info(self):
        return {'name':        'rlp',
                'title':       'RLP Harvester',
                'description': 'A CKAN Harvester for Rhineland-Palatinate solving data compatibility problems.'}

    def __init__(self):
        config_dir = os.path.dirname(os.path.abspath(__file__))
        config = ConfigParser.RawConfigParser()
        config.read(config_dir + '/config.ini')
        schema_url = config.get('URLs', 'schema')
        groups_url = config.get('URLs', 'groups')
        self.schema = json.loads(urllib2.urlopen(schema_url).read())
        self.govdata_groups = json.loads(urllib2.urlopen(groups_url).read())

    def import_stage(self, harvest_object):
        package_dict = json.loads(harvest_object.content)

        if not package_dict['extras']['content_type'] == 'datensatz':
            return  # skip dataset

        package_dict['type'] = 'datensatz'
        for resource in package_dict['resources']:
            if resource['format'].lower() != 'pdf':
                package_dict['type'] = 'dokument'
                break

        assert_author_fields(package_dict, package_dict['point_of_contact'],
                             package_dict['point_of_contact_address']['email'])

        package_dict['extras']['metadata_original_portal'] = 'http://daten.rlp.de'
        package_dict['extras']['sector'] = 'oeffentlich'

        for extra_field in self.schema['properties']['extras']['properties'].keys():
            if extra_field in package_dict:
                package_dict['extras'][extra_field] = package_dict[extra_field]
                del package_dict[extra_field]

        package_dict['license_id'] = package_dict['extras']['terms_of_use']['license_id']

        if 'justiz' in package_dict['groups']:
            package_dict['groups'].append('gesetze_justiz')
            package_dict['groups'].remove('justiz')

        if 'transport' in package_dict['groups']:
            package_dict['groups'].append('transport_verkehr')
            package_dict['groups'].remove('transport')

        package_dict['groups'] = [group for group in package_dict['groups'] if group in self.govdata_groups]

        super(RLPCKANHarvester, self).import_stage(harvest_object)
