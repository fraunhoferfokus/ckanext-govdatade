#!/usr/bin/python
# -*- coding: utf8 -*-

from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.govdatade.harvesters.translator import translate_groups

import ConfigParser
import json
import logging
import os
import urllib2
import uuid


config = ConfigParser.RawConfigParser()
config_dir = os.path.dirname(os.path.abspath(__file__))
config.read(config_dir + '/config.ini')

logfile_path = config.get('Logger', 'logfile')
logfile_directory = os.path.dirname(logfile_path)
if logfile_directory and not os.path.exists(logfile_directory):
    os.makedirs(logfile_directory)

formatter = logging.Formatter(config.get('Logger', 'format'))
fh = logging.FileHandler(logfile_path)
fh.setFormatter(formatter)

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(fh)


def assert_author_fields(package_dict, author_alternative, author_email_alternative):
    """Ensures that the author field is set."""

    if not 'author' in package_dict or not package_dict['author']:
        package_dict['author'] = author_alternative

    if not 'author_email' in package_dict or not package_dict['author_email']:
        package_dict['author_email'] = author_email_alternative

    if not package_dict['author']:
        raise ValueError('There is no author for package %s' % package_dict['id'])


def resolve_json_incosistency(dataset):
    return dataset


class GroupCKANHarvester(CKANHarvester):
    """An extended CKAN harvester that also imports remote groups, for that api version 1 is enforced"""

    api_version = 1
    """Enforce API version 1 for enabling group import"""

    def __init__(self):
        schema_url = config.get('URLs', 'schema')
        groups_url = config.get('URLs', 'groups')
        self.schema = json.loads(urllib2.urlopen(schema_url).read())
        self.govdata_groups = json.loads(urllib2.urlopen(groups_url).read())

    def _set_config(self, config_str):
        """Enforce API version 1 for enabling group import"""
        if config_str:
            self.config = json.loads(config_str)
        else:
            self.config = {}
        self.api_version = 1
        self.config['api_version'] = 1
        self.config['force_all'] = True
        self.config['remote_groups'] = 'only_local'

    def import_stage(self, harvest_object):
        package_dict = json.loads(harvest_object.content)
        try:
            self.amend_package(package_dict)
        except ValueError, e:
            self._save_object_error(str(e), harvest_object)
            log.error('Rostock: ' + str(e))
            return
        harvest_object.content = json.dumps(package_dict)
        super(GroupCKANHarvester, self).import_stage(harvest_object)


class RostockCKANHarvester(GroupCKANHarvester):
    """A CKAN Harvester for Rostock solving data compatibility problems."""

    def info(self):
        return {'name':        'rostock',
                'title':       'Rostock Harvester',
                'description': 'A CKAN Harvester for Rostock solving data'
                'compatibility problems.'}

    def amend_package(self, package):
        portal = 'http://www.opendata-hro.de'
        package['extras']['metadata_original_portal'] = portal
        package['name'] = package['name'] + '-hro'

    def import_stage(self, harvest_object):
        package_dict = json.loads(harvest_object.content)
        try:
            self.amend_package(package_dict)
        except ValueError, e:
            self._save_object_error(str(e), harvest_object)
            log.error('Rostock: ' + str(e))
            return
        harvest_object.content = json.dumps(package_dict)
        super(RostockCKANHarvester, self).import_stage(harvest_object)


class HamburgCKANHarvester(GroupCKANHarvester):
    """A CKAN Harvester for Hamburg solving data compatibility problems."""

    def info(self):
        return {'name':        'hamburg',
                'title':       'Hamburg Harvester',
                'description': 'A CKAN Harvester for Hamburg solving data compatibility problems.'}

    def amend_package(self, package):

        # fix usage of hyphen, the schema group names use underscores
        package['groups'] = [name.replace('-', '_') for name in package['groups']]

        # add tag for better searchability
        package['tags'].append(u'Hamburg')
        assert_author_fields(package, package['maintainer'],
                             package['maintainer_email'])

    def import_stage(self, harvest_object):
        package_dict = json.loads(harvest_object.content)
        try:
            self.amend_package(package_dict)
        except ValueError, e:
            self._save_object_error(str(e), harvest_object)
            log.error('Hamburg: ' + str(e))
            return
        harvest_object.content = json.dumps(package_dict)
        super(HamburgCKANHarvester, self).import_stage(harvest_object)


class BerlinCKANHarvester(GroupCKANHarvester):
    """A CKAN Harvester for Berlin sovling data compatibility problems."""

    def info(self):
        return {'name':        'berlin',
                'title':       'Berlin Harvester',
                'description': 'A CKAN Harvester for Berlin solving data compatibility problems.'}

    def amend_package(self, package):

        extras = package['extras']

        if package['license_id'] == '':
            package['license_id'] = 'notspecified'

        # if sector is not set, set it to 'oeffentlich' (default)
        if not extras.get('sector'):
            extras['sector'] = 'oeffentlich'

        if package['extras']['sector'] != 'oeffentlich':
            return False

        valid_types = ['datensatz', 'dokument', 'app']
        if not package.get('type') or package['type'] not in valid_types:
            package['type'] = 'datensatz'

        package['groups'] = translate_groups(package['groups'], 'berlin')
        default_portal = 'http://datenregister.berlin.de'
        if not extras.get('metadata_original_portal'):
            extras['metadata_original_portal'] = default_portal
        return True

    def import_stage(self, harvest_object):
        package_dict = json.loads(harvest_object.content)
        valid = self.amend_package(package_dict)

        if not valid:
            return  # drop package

        harvest_object.content = json.dumps(package_dict)
        super(BerlinCKANHarvester, self).import_stage(harvest_object)


class RLPCKANHarvester(GroupCKANHarvester):
    """A CKAN Harvester for Rhineland-Palatinate sovling data compatibility problems."""

    def info(self):
        return {'name':        'rlp',
                'title':       'RLP Harvester',
                'description': 'A CKAN Harvester for Rhineland-Palatinate solving data compatibility problems.'}

    def __init__(self):
        schema_url = config.get('URLs', 'schema')
        groups_url = config.get('URLs', 'groups')

        self.schema = json.loads(urllib2.urlopen(schema_url).read())
        self.govdata_groups = json.loads(urllib2.urlopen(groups_url).read())

    def amend_package(self, package_dict):
        # manually set package type
        if all([resource['format'].lower() == 'pdf' for resource in package_dict['resources']]):
            package_dict['type'] = 'dokument'
        else:
            package_dict['type'] = 'datensatz'

        assert_author_fields(package_dict, package_dict['point_of_contact'],
                             package_dict['point_of_contact_address']['email'])

        package_dict['extras']['metadata_original_portal'] = 'http://daten.rlp.de'
        package_dict['extras']['sector'] = 'oeffentlich'

        # the extra fields are present as CKAN core fields in the remote
        # instance: copy all content from these fields into the extras field
        for extra_field in self.schema['properties']['extras']['properties'].keys():
            if extra_field in package_dict:
                package_dict['extras'][extra_field] = package_dict[extra_field]
                del package_dict[extra_field]

        # convert license cc-by-nc to cc-nc
        if package_dict['extras']['terms_of_use']['license_id'] == 'cc-by-nc':
            package_dict['extras']['terms_of_use']['license_id'] = 'cc-nc'

        package_dict['license_id'] = package_dict['extras']['terms_of_use']['license_id']

        # GDI related patch
        if 'gdi-rp' in package_dict['groups']:
            package_dict['type'] = 'datensatz'

        # map these two group names to schema group names
        if 'justiz' in package_dict['groups']:
            package_dict['groups'].append('gesetze_justiz')
            package_dict['groups'].remove('justiz')

        if 'transport' in package_dict['groups']:
            package_dict['groups'].append('transport_verkehr')
            package_dict['groups'].remove('transport')

        # filter illegal group names
        package_dict['groups'] = [group for group in package_dict['groups'] if group in self.govdata_groups]

    def import_stage(self, harvest_object):
        package_dict = json.loads(harvest_object.content)

        dataset = package_dict['extras']['content_type'].lower() == 'datensatz'
        if not dataset and not 'gdi-rp' in package_dict['groups']:
            return  # skip all non-datasets for the time being

        try:
            self.amend_package(package_dict)
        except ValueError, e:
            self._save_object_error(str(e), harvest_object)
            log.error('RLP: ' + str(e))
            return

        harvest_object.content = json.dumps(package_dict)
        super(RLPCKANHarvester, self).import_stage(harvest_object)


class JSONDumpBaseCKANHarvester(GroupCKANHarvester):

    def info(self):
        return {'name':        'base',
                'title':       'Base Harvester',
                'description': 'A Base CKAN Harvester for CKANs which return a JSON dump file.'}

    def gather_stage(self, harvest_job):
        self._set_config(harvest_job.source.config)
        # Request all remote packages
        try:
            content = self._get_content(harvest_job.source.url)
        except Exception, e:
            self._save_gather_error('Unable to get content for URL: %s: %s' % (harvest_job.source.url, str(e)), harvest_job)
            return None

        object_ids = []

        packages = json.loads(content)
        for package in packages:
            obj = HarvestObject(guid=package['name'], job=harvest_job)
            obj.content = json.dumps(package)
            obj.save()
            object_ids.append(obj.id)

        if object_ids:
            return object_ids
        else:
            self._save_gather_error('No packages received for URL: %s' % harvest_job.source.url,
                                    harvest_job)
            return None

    def fetch_stage(self, harvest_object):
        self._set_config(harvest_object.job.source.config)

        if harvest_object.content:
            return True
        else:
            return False


class BremenCKANHarvester(JSONDumpBaseCKANHarvester):
    '''
    A CKAN Harvester for Bremen. The Harvester retrieves a JSON dump,
    which will be loaded to CKAN.
    '''
    def info(self):
        return {'name':        'bremen',
                'title':       'Bremen CKAN Harvester',
                'description': 'A CKAN Harvester for Bremen.'}

    def amend_package(self, package):
        '''
        This function fixes some differences in the datasets retrieved from Bremen and our schema such as:
        - fix groups
        - set metadata_original_portal
        - fix terms_of_use
        - copy veroeffentlichende_stelle to maintainer
        - set spatial text
        '''

        #set metadata original portal
        package['extras']['metadata_original_portal'] = 'http://daten.bremen.de/sixcms/detail.php?template=export_daten_json_d'

        # set correct groups
        if not package['groups']:
            package['groups'] = []
        package['groups'] = translate_groups(package['groups'], 'bremen')

        #copy veroeffentlichende_stelle to maintainer
        if 'contacts' in package['extras']:
            quelle = filter(lambda x: x['role'] == 'veroeffentlichende_stelle', package['extras']['contacts'])[0]
            package['maintainer'] = quelle['name']
            package['maintainer_email'] = quelle['email']

        #fix typos in terms of use
        if 'terms_of_use' in package['extras']:
            self.fix_terms_of_use(package['extras']['terms_of_use'])
            #copy license id
            package['license_id'] = package['extras']['terms_of_use']['license_id']
        else:
            package['license_id'] = u'notspecified'

        if not "spatial-text" in package["extras"]:
            package["extras"]["spatial-text"] = 'Bremen 04 0 11 000'

        #generate id based on OID namespace and package name, this makes sure,
        #that packages with the same name get the same id
        package['id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(BremenCKANHarvester, self).import_stage(harvest_object)

    def fix_terms_of_use(self, terms_of_use):
        terms_of_use['license_id'] = terms_of_use['licence_id']
        del(terms_of_use['licence_id'])
        terms_of_use['license_url'] = terms_of_use['licence_url']
        del(terms_of_use['licence_url'])


class BayernCKANHarvester(JSONDumpBaseCKANHarvester):
    '''
    A CKAN Harvester for Bavaria. The Harvester retrieves a JSON dump,
    which will be loaded to CKAN.
    '''

    def info(self):
        return {'name':        'bayern',
                'title':       'Bavarian CKAN Harvester',
                'description': 'A CKAN Harvester for Bavaria.'}

    def amend_package(self, package):
        if len(package['name']) > 100:
            package['name'] = package['name'][:100]
        if not package['groups']:
            package['groups'] = []

        #copy autor to author
        quelle = {}
        if 'contacts' in package['extras']:
            quelle = filter(lambda x: x['role'] == 'autor', package['extras']['contacts'])[0]

        if not package['author'] and quelle:
            package['author'] = quelle['name']
        if not package['author_email']:
            if 'email' in quelle:
                package['author_email'] = quelle['email']

        if not "spatial-text" in package["extras"].keys():
            package["extras"]["spatial-text"] = 'Bayern 09'
        for r in package['resources']:
            r['format'] = r['format'].upper()

        #generate id based on OID namespace and package name, this makes sure,
        #that packages with the same name get the same id
        package['id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(BayernCKANHarvester, self).import_stage(harvest_object)


class MoersCKANHarvester(JSONDumpBaseCKANHarvester):
    """A CKAN Harvester for Moers solving data compatibility problems."""

    def info(self):
        return {'name':        'moers',
                'title':       'Moers Harvester',
                'description': 'A CKAN Harvester for Moers solving data compatibility problems.'}

    def amend_package(self, package):

        publishers = filter(lambda x: x['role'] == 'veroeffentlichende_stelle', package['extras']['contacts'])
        maintainers = filter(lambda x: x['role'] == 'ansprechpartner', package['extras']['contacts'])

        if not publishers:
            raise ValueError('There is no author email for package %s' % package_dict['id'])

        package['id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))
        package['name'] = package['name'].lower()

        if 'moers' not in package['title'].lower():
            package['title'] = package['title'] + ' Moers'

        package['author'] = 'Stadt Moers'
        package['author_email'] = publishers[0]['email']

        if maintainers:
            package['maintainer'] = maintainers[0]['name']
            package['maintainer_email'] = maintainers[0]['email']

        package['license_id'] = package['extras']['terms_of_use']['license_id']
        package['extras']['metadata_original_portal'] = 'http://www.offenedaten.moers.de/'

        if not "spatial-text" in package["extras"].keys():
            package["extras"]["spatial-text"] = '05 1 70 024 Moers'

        if isinstance(package['tags'], basestring):
            if not package['tags']:  # if tags was set to "" or null
                package['tags'] = []
            else:
                package['tags'] = [package['tags']]
        package['tags'].append('moers')

        for resource in package['resources']:
            resource['format'] = resource['format'].replace('text/comma-separated-values', 'XLS')
            resource['format'] = resource['format'].replace('application/json', 'JSON')
            resource['format'] = resource['format'].replace('application/xml', 'XML')

    def import_stage(self, harvest_object):
        package_dict = json.loads(harvest_object.content)
        try:
            self.amend_package(package_dict)
        except ValueError, e:
            self._save_object_error(str(e), harvest_object)
            log.error('Moers: ' + str(e))
            return
        harvest_object.content = json.dumps(package_dict)
        super(MoersCKANHarvester, self).import_stage(harvest_object)


class GovAppsHarvester(JSONDumpBaseCKANHarvester):
    '''
    A CKAN Harvester for GovApps. The Harvester retrieves a JSON dump,
    which will be loaded to CKAN.
    '''

    def info(self):
        return {'name':        'govapps',
                'title':       'GovApps Harvester',
                'description': 'A CKAN Harvester for GovApps.'}

    def amend_package(self, package):
        if not package['groups']:
            package['groups'] = []
        #fix groups
        if not package['groups']:
            package['groups'] = []
        package['groups'] = [x for x in translate_groups(package['groups'], 'govapps') if len(x) > 0]

        #generate id based on OID namespace and package name, this makes sure,
        #that packages with the same name get the same id
        package['id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(GovAppsHarvester, self).import_stage(harvest_object)


class DatahubCKANHarvester(GroupCKANHarvester):
    """A CKAN Harvester for Datahub IO importing a small set of packages."""

    portal = 'http://datahub.io/'

    valid_packages = ['hbz_unioncatalog', 'lobid-resources',
                      'deutsche-nationalbibliografie-dnb',
                      'dnb-gemeinsame-normdatei']

    def info(self):
        return {'name':        'datahub',
                'title':       'Datahub IO Harvester',
                'description': 'A CKAN Harvester for Datahub IO importing a '
                               'small set of packages.'}

    def fetch_stage(self, harvest_object):
        log.debug('In CKANHarvester fetch_stage')
        self._set_config(harvest_object.job.source.config)

        if harvest_object.guid not in DatahubCKANHarvester.valid_packages:
            return None

        # Get source URL
        url = harvest_object.source.url.rstrip('/')
        url = url + self._get_rest_api_offset() + '/package/'
        url = url + harvest_object.guid

        # Get contents
        try:
            content = self._get_content(url)
        except Exception, e:
            self._save_object_error('Unable to get content for package:'
                                    '%s: %r' % (url, e), harvest_object)
            return None

        # Save the fetched contents in the HarvestObject
        harvest_object.content = content
        harvest_object.save()
        return True

    def package_valid(self, package_name):
        return package_name in DatahubCKANHarvester.valid_packages

    def amend_package(self, package_dict):
        portal = DatahubCKANHarvester.portal

        # Currently, only the description is displayed. Some datasets only have
        # a descriptive name, but no description. Hence, it is copied if unset.
        for resource in package_dict['resources']:
            description = resource['description'].lower()
            name = resource['name']

            name_valid = name and not name.isspace()
            description_invalid = not description or description.isspace()
            type_only = 'rdf/xml' in description

            if description_invalid or (type_only and name_valid):
                resource['description'] = resource['name']

        package_dict['extras']['metadata_original_portal'] = portal
        package_dict['groups'].append('bildung_wissenschaft')
        package_dict['groups'] = [group for group in package_dict['groups']
                                  if group in self.govdata_groups]
