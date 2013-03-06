from ckan.lib.helpers import json
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.govdatade.harvesters.translator import translate_groups

import ConfigParser
import logging
import urllib2


AUTHOR = 'author'
AUTHOR_EMAIL = 'author_email'
CONTENT_TYPE = 'content_type'
EMAIL = 'email'
EXTRAS = 'extras'
FORMAT = 'format'
GROUPS = 'groups'
LICENSE_ID = 'license_id'
MAINTAINER = 'maintainer'
MAINTAINER_EMAIL = 'maintainer_email'
METADATA_ORIGINAL_PORTAL = 'metadata_original_portal'
POINT_OF_CONTACT = 'point_of_contact'
POINT_OF_CONTACT_ADDRESS = 'point_of_contact_address'
PROPERTIES = 'properties'
RESOURCES = 'resources'
SECTOR = 'sector'
TAGS = 'tags'
TERMS_OF_USE = 'terms_of_use'
TYPE = 'type'

log = logging.getLogger(__name__)


def assert_author_fields(package_dict, author_alternative, author_email_alternative):
    """Ensures that the author field is set."""

    if not package_dict[AUTHOR]:
        package_dict[AUTHOR] = author_alternative

    if not package_dict[AUTHOR_EMAIL]:
        package_dict[AUTHOR_EMAIL] = author_email_alternative

    if not package_dict[AUTHOR]:
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

        package_dict[GROUPS] = [name.replace('-', '_') for name in package_dict[GROUPS]]
        package_dict[TAGS].append(u'Hamburg')
        assert_author_fields(package_dict, package_dict[MAINTAINER], package_dict[MAINTAINER_EMAIL])

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

        if package_dict[LICENSE_ID] == '':
            package_dict[LICENSE_ID] = 'notspecified'

        package_dict[GROUPS] = translate_groups(package_dict[GROUPS], 'berlin')

        harvest_object.content = json.dumps(package_dict)
        super(BerlinCKANHarvester, self).import_stage(harvest_object)


class RLPCKANHarvester(GroupCKANHarvester):
    """A CKAN Harvester for Rhineland-Palatinate sovling data compatibility problems."""

    def info(self):
        return {'name':        'rlp',
                'title':       'RLP Harvester',
                'description': 'A CKAN Harvester for Rhineland-Palatinate solving data compatibility problems.'}

    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read('config.ini')
        schema_url = config.get('URLs', 'schema')
        groups_url = config.get('URLs', 'groups')
        self.schema = json.loads(urllib2.urlopen(schema_url).read())
        self.govdata_groups = json.loads(urllib2.urlopen(groups_url).read())

    def import_stage(self, harvest_object):
        package_dict = json.loads(harvest_object.content)

        if not package_dict[EXTRAS][CONTENT_TYPE] == 'datensatz':
            return  # skip dataset

        package_dict[TYPE] = 'datensatz'
        for resource in package_dict[RESOURCES]:
            if resource[FORMAT].lower() != 'pdf':
                package_dict[TYPE] = 'dokument'
                break

        assert_author_fields(package_dict, package_dict[POINT_OF_CONTACT],
                             package_dict[POINT_OF_CONTACT_ADDRESS][EMAIL])

        package_dict[EXTRAS][METADATA_ORIGINAL_PORTAL] = 'http://daten.rlp.de'
        package_dict[EXTRAS][SECTOR] = 'oeffentlich'

        for extra_field in self.schema[PROPERTIES][EXTRAS][PROPERTIES].keys():
            if extra_field in package_dict:
                package_dict[EXTRAS][extra_field] = package_dict[extra_field]
                del package_dict[extra_field]

        package_dict[LICENSE_ID] = package_dict[EXTRAS][TERMS_OF_USE][LICENSE_ID]

        if 'justiz' in package_dict[GROUPS]:
            package_dict[GROUPS].append('gesetze_justiz')
            package_dict[GROUPS].remove('justiz')

        if 'transport' in package_dict[GROUPS]:
            package_dict[GROUPS].append('transport_verkehr')
            package_dict[GROUPS].remove('transport')

        package_dict[GROUPS] = [group for group in package_dict[GROUPS] if group in self.govdata_groups]

        super(RLPCKANHarvester, self).import_stage(harvest_object)
