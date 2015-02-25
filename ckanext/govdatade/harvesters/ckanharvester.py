#!/usr/bin/python
# -*- coding: utf8 -*-

from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.govdatade.harvesters.translator import translate_groups
from ckanext.govdatade.util import iterate_local_datasets
from ckanext.govdatade.validators.link_checker import LinkChecker
from ckanext.govdatade import CONFIG

from ckan import model
from ckan.logic import get_action
from ckan.logic.schema import default_package_schema
from ckan.model import Session

import json
import logging
import urllib2
import uuid
import ckanapi
import datetime

log = logging.getLogger(__name__)


def assert_author_fields(package_dict, author_alternative,
                         author_email_alternative):
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
    """
    An extended CKAN harvester that also imports remote groups, for that api
    version 1 is enforced
    """

    api_version = 1
    """Enforce API version 1 for enabling group import"""

    def __init__(self):
        schema_url = 'https://raw.githubusercontent.com/fraunhoferfokus/ogd-metadata/master/OGPD_JSON_Schema.json'  # CONFIG.get('URLs', 'schema')
        groups_url = 'https://raw.githubusercontent.com/fraunhoferfokus/ogd-metadata/master/kategorien/deutschland.json'  # CONFIG.get('URLs', 'groups')
        self.schema = json.loads(urllib2.urlopen(schema_url).read())
        self.govdata_groups = json.loads(urllib2.urlopen(groups_url).read())
        self.link_checker = LinkChecker()

    def _set_config(self, config_str, user=None):
        """Enforce API version 1 for enabling group import"""
        if config_str:
            self.config = json.loads(config_str)
        else:
            self.config = {}
        self.api_version = 1
        self.config['api_version'] = 1
        self.config['force_all'] = True
        self.config['remote_groups'] = 'only_local'
        self.config['user'] = 'harvest'
        
        if(user):
            self.config['user'] = user
            
    def import_stage(self, harvest_object):
        log.debug('super start verify'+ harvest_object.content)
        package_dict = json.loads(harvest_object.content)
        delete = self.link_checker.process_record(package_dict)
        # deactivated until broken links are fixed
        if delete:
            package_dict['state'] = 'deleted'
        else:
            if 'deprecated' not in package_dict['tags']:
                package_dict['state'] = 'active'

        harvest_object.content = json.dumps(package_dict)
        log.debug('ckanharvester86 '+harvest_object.id)
        super(GroupCKANHarvester, self).import_stage(harvest_object)


class GovDataHarvester(GroupCKANHarvester):
    """The base harvester for GovData.de perfoming remote synchonization."""

    def build_context(self):
        return {'model': model,
                'session': Session,
                'user': u'harvest',
                'schema': default_package_schema(),
                'validate': False,
                'api_version': 1}

    def portal_relevant(self, portal):
        def condition_check(dataset):
            for extra in dataset['extras']:
                if extra['key'] == 'metadata_original_portal':
                    value = extra['value']
                    value = value.lstrip('"').rstrip('"')
                    return value == portal

            return False

        return condition_check

    def delete_deprecated_datasets(self, context, remote_dataset_names):
        package_update = get_action('package_update')

        local_datasets = iterate_local_datasets(context)
        filtered = filter(self.portal_relevant(self.PORTAL), local_datasets)
        local_dataset_names = map(lambda dataset: dataset['name'], filtered)

        deprecated = set(local_dataset_names) - set(remote_dataset_names)
        log.info('Found %s deprecated datasets.' % len(deprecated))

        for local_dataset in filtered:
            if local_dataset['name'] in deprecated:
                local_dataset['state'] = 'deleted'
                local_dataset['tags'].append({'name': 'deprecated'})
                package_update(context, local_dataset)

    def verify_transformer(self, remote_dataset):
        """ Based on metadata_transformer, this method checks, if a dataset should be imported"""
        registry = ckanapi.RemoteCKAN('http://localhost:80/ckan')
        remote_dataset = json.loads(remote_dataset)
        remote_dataset_extras = remote_dataset['extras']
        if 'metadata_original_id' in remote_dataset_extras:
            orig_id = remote_dataset_extras['metadata_original_id']
            try:
                local_search_result = registry.action.package_search(q='metadata_original_id:"' + orig_id + '"')
                if local_search_result['count'] == 0:
                    log.debug('Did not find this original id. Import accepted.')
                    return True
                if local_search_result['count'] == 1:
                    log.debug('Found duplicate entry')
                    local_dataset = local_search_result['results'][0]
                    local_dataset_extras = local_dataset['extras']
                    if 'metadata_transformer' in [entry['key'] for entry in local_dataset_extras]:
                        log.debug('Found metadata_transformer')
                        local_transformer = None
                        local_portal = None
                        for entry in local_dataset_extras:
                            if entry['key'] == 'metadata_transformer':
                                value = entry['value']
                                local_transformer = value.lstrip('"').rstrip('"')
                                log.debug('Found local metadata transformer')
			    if entry['key'] == 'metadata_original_portal':
				tmp_value = entry['value']
				local_portal = tmp_value.lstrip('"').rstrip('"')

                        remote_transformer = remote_dataset_extras['metadata_transformer']
                        if remote_transformer == local_transformer or remote_transformer == 'harvester':
                            #TODO this is temporary for gdi-de
			    if local_portal == 'http://ims.geoportal.de/':
				log.debug('Found geoportal, accept import.')
				return True
			    log.debug('Remote metadata transformer equals local transformer -> skipping')
                            return False
                        elif remote_transformer == 'author' and local_transformer == 'harvester':
                            log.debug(
                                'Remote metadata transformer equals author and local equals harvester -> importing.')
                            return True
                        else:
                            log.debug('unknown value for remote metadata_transformer -> skipping.')
                            return False
                    else:
                        if 'metadata_modified' in remote_dataset:
                            dt_format = "%Y-%m-%dT%H:%M:%S.%f"
                            remote_dt = datetime.datetime.strptime(remote_dataset['metadata_modified'], dt_format)
                            local_dt = datetime.datetime.strptime(local_dataset['metadata_modified'], dt_format)
                            if remote_dt < local_dt:
                                log.debug('remote dataset precedes local dataset -> skipping.')
                                return False
                            elif remote_dt == local_dt:
                                log.debug('remote dataset equals local dataset -> skipping.')
                                return False
                            else:
                                log.debug('local dataset precedes remote dataset -> importing.')
                                # TODO do I have to delete other dataset?
                                return True
                        else:
                            log.debug(
                                'Found duplicate entry but remote dataset does not contain metadata_modified -> skipping.')
                            return False
            except Exception as e:
                log.error(e)
        else:
            log.debug('no metadata_original_id. Importing accepted.')
            return True

    def gather_stage(self, harvest_job):
        """Retrieve local datasets for synchronization."""

        self._set_config(harvest_job.source.config)
        content = self._get_content(harvest_job.source.url)

        base_url = harvest_job.source.url.rstrip('/')
        base_rest_url = base_url + self._get_rest_api_offset()
        url = base_rest_url + '/package'

        try:
            content = self._get_content(url)
        except Exception, e:
            error = 'Unable to get content for URL: %s: %s' % (url, str(e))
            self._save_gather_error(error, harvest_job)
            return None

        context = self.build_context()
        remote_datasets = json.loads(content)
        # remote_dataset_names = map(lambda d: d['name'], remote_datasets)
        # self.delete_deprecated_datasets(context, remote_dataset_names)

        return super(GovDataHarvester, self).gather_stage(harvest_job)

    def import_stage(self, harvest_object):
        log.debug('start verify '+harvest_object.content)
        to_import = self.verify_transformer(harvest_object.content)
        if to_import:
            log.debug('govdataharvester227 '+harvest_object.id)
            super(GovDataHarvester, self).import_stage(harvest_object)


class RostockCKANHarvester(GovDataHarvester):
    """A CKAN Harvester for Rostock solving data compatibility problems."""

    PORTAL = 'http://www.opendata-hro.de'

    def info(self):
        return {'name': 'rostock',
                'title': 'Rostock Harvester',
                'description': 'A CKAN Harvester for Rostock solving data'
                               'compatibility problems.'}

    def amend_package(self, package):
        portal = 'http://www.opendata-hro.de'
        package['extras']['metadata_original_portal'] = portal
        package['name'] = package['name'] + '-hro'
        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

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
        return {'name': 'hamburg',
                'title': 'Hamburg Harvester',
                'description': 'A CKAN Harvester for Hamburg solving data compatibility problems.'}

    def amend_package(self, package):
        # check if import is desired
        if package['type'] == 'document':
            # check if tag 'govdata' exists
            if not [tag for tag in package['tags'] if tag.lower() == 'govdata']:
                log.debug('Found invalid package')
                return False
            package['type'] = 'dokument'
        # check if import is desired
        elif package['type'] == 'dokument':
            # check if tag 'govdata' exists
            if not [tag for tag in package['tags'] if tag.lower() == 'govdata']:
                log.debug('Found invalid package')
                return False
        elif package['type'] == 'dataset':
            package['type'] = 'datensatz'

        extras = package['extras']
        # fix groups
        log.debug("Before: ")
        log.debug(package['groups'])
        package['groups'] = translate_groups(package['groups'], 'hamburg')
        log.debug("After: ")
        log.debug(package['groups'])
        # set original portal
        default_portal = 'http://suche.transparenz.hamburg.de/'
        if not extras.get('metadata_original_portal'):
            extras['metadata_original_portal'] = default_portal

        assert_author_fields(package, package['maintainer'],
                             package['maintainer_email'])

        return True

    def import_stage(self, harvest_object):
        package_dict = json.loads(harvest_object.content)
        try:
            valid = self.amend_package(package_dict)
            if not valid:
                return  # drop package
        except ValueError, e:
            self._save_object_error(str(e), harvest_object)
            log.error('Hamburg: ' + str(e))
            return
        harvest_object.content = json.dumps(package_dict)
        super(HamburgCKANHarvester, self).import_stage(harvest_object)


class BerlinCKANHarvester(GovDataHarvester):
    """A CKAN Harvester for Berlin sovling data compatibility problems."""
    PORTAL = 'http://datenregister.berlin.de/'

    def info(self):
        return {'name': 'berlin',
                'title': 'Berlin Harvester',
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
        for resource in package['resources']:
            resource['format'] = resource['format'].lower()
        return True

    def import_stage(self, harvest_object):
        package_dict = json.loads(harvest_object.content)
        valid = self.amend_package(package_dict)

        if not valid:
            return  # drop package

        harvest_object.content = json.dumps(package_dict)
        super(BerlinCKANHarvester, self).import_stage(harvest_object)


class RLPCKANHarvester(GovDataHarvester):
    """A CKAN Harvester for Rhineland-Palatinate sovling data compatibility problems."""

    def info(self):
        return {'name': 'rlp',
                'title': 'RLP Harvester',
                'description': 'A CKAN Harvester for Rhineland-Palatinate solving data compatibility problems.'}

    def __init__(self):

        schema_url = 'https://raw.githubusercontent.com/fraunhoferfokus/ogd-metadata/master/OGPD_JSON_Schema.json'  # CONFIG.get('URLs', 'schema')
        groups_url = 'https://raw.githubusercontent.com/fraunhoferfokus/ogd-metadata/master/kategorien/deutschland.json'  # CONFIG.get('URLs', 'groups')

        self.schema = json.loads(urllib2.urlopen(schema_url).read())
        self.govdata_groups = json.loads(urllib2.urlopen(groups_url).read())
        self.link_checker = LinkChecker()

    def amend_package(self, package_dict):
        # manually set package type
        if all([resource['format'].lower() == 'pdf' for resource in package_dict['resources']]):
            package_dict['type'] = 'dokument'
        else:
            package_dict['type'] = 'datensatz'

        for resource in package_dict['resources']:
            resource['format'] = resource['format'].lower()

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


class DatahubCKANHarvester(GroupCKANHarvester):
    """A CKAN Harvester for Datahub IO importing a small set of packages."""

    portal = 'http://datahub.io/'

    valid_packages = ['hbz_unioncatalog', 'lobid-resources',
                      'deutsche-nationalbibliografie-dnb',
                      'dnb-gemeinsame-normdatei']

    def info(self):
        return {'name': 'datahub',
                'title': 'Datahub IO Harvester',
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

        package_dict['type'] = 'datensatz'
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

        for resource in package_dict['resources']:
            resource['format'] = resource['format'].lower()

        package_dict['extras']['metadata_original_portal'] = portal
        package_dict['groups'].append('bildung_wissenschaft')
        package_dict['groups'] = [group for group in package_dict['groups']
                                  if group in self.govdata_groups]


class KoelnCKANHarvester(GroupCKANHarvester):
    '''
    A CKAN Harvester for Koeln. The Harvester retrieves a JSON dump,
    which will be loaded to CKAN.
    '''

    city = 'Koeln'

    def info(self):
        return {'name': 'koeln',
                'title': 'Koeln CKAN Harvester',
                'description': 'A CKAN Harvester for Koeln.'}


    def gather_stage(self, harvest_job):
        """Retrieve datasets"""

        log.debug('In ' + self.city + 'CKANHarvester gather_stage (%s)' % harvest_job.source.url)
        package_ids = []
        self._set_config(None)

        base_url = harvest_job.source.url.rstrip('/')
        package_list_url = base_url + '/3/action/package_list'
        content = self._get_content(package_list_url)

        content_json = json.loads(content)
        package_ids = content_json['result']

        try:
            object_ids = []
            if len(package_ids):
                for package_id in package_ids:
                    obj = HarvestObject(guid=package_id, job=harvest_job)
                    obj.save()
                    object_ids.append(obj.id)
                return object_ids

            else:
                self._save_gather_error('No packages received for URL: %s' % url,
                                        harvest_job)
                return None
        except Exception, e:
            self._save_gather_error('%r' % e.message, harvest_job)


    def fetch_stage(self, harvest_object):
        log.debug('In ' + self.city +'CKANHarvester fetch_stage')
        self._set_config(None)

        # Get contents
        package_get_url = ''
        try:
            base_url = harvest_object.source.url.rstrip('/')

            package_get_url = base_url + '/3/ogdp/action/package_show?id=' + harvest_object.guid
            content = self._get_content(package_get_url.encode("utf-8"))
            package = json.loads(content)
            harvest_object.content = json.dumps(package['result'][0])
            harvest_object.save()

        except Exception, e:
            self._save_object_error('Unable to get content for package: %s: %r' % \
                                    (package_get_url, e), harvest_object)
            return None

        return True


    def import_stage(self, harvest_object):
        package_dict = json.loads(harvest_object.content)
        try:
            self.amend_package(package_dict)
        except ValueError, e:
            self._save_object_error(str(e), harvest_object)
            log.error(self.city +': ' + str(e))
            return

        harvest_object.content = json.dumps(package_dict)
        super(KoelnCKANHarvester, self).import_stage(harvest_object)


    def amend_package(self, package):
        # map these two group names to schema group names
        out = []
        if 'Geo' in package['groups']:
            package['groups'].append('geo')
            package['groups'].remove('Geo')

        if 'Bildung und Wissenschaft' in package['groups']:
            package['groups'].append(u'bildung_wissenschaft')
            package['groups'].remove('Bildung und Wissenschaft')

        if 'Gesetze und Justiz' in package['groups']:
            package['groups'].append(u'gesetze_justiz')
            package['groups'].remove('Gesetze und Justiz')

        if 'Gesundheit' in package['groups']:
            package['groups'].append(u'gesundheit')
            package['groups'].remove('Gesundheit')

        if 'Infrastruktur' in package['groups']:
            package['groups'].append(u'infrastruktur_bauen_wohnen')
            package['groups'].remove('Infrastruktur')
            package['groups'].remove('Bauen und Wohnen')

        if 'Kultur' in package['groups']:
            package['groups'].append(u'kultur_freizeit_sport_tourismus')
            package['groups'].remove('Kultur')
            package['groups'].remove('Freizeit')
            package['groups'].remove('Sport und Tourismus')

        if 'Politik und Wahlen' in package['groups']:
            package['groups'].append(u'politik_wahlen')
            package['groups'].remove('Politik und Wahlen')

        if 'Soziales' in package['groups']:
            package['groups'].append(u'soziales')
            package['groups'].remove('Soziales')

        if 'Transport und Verkehr' in package['groups']:
            package['groups'].append(u'transport_verkehr')
            package['groups'].remove('Transport und Verkehr')

        if 'Umwelt und Klima' in package['groups']:
            package['groups'].append(u'umwelt_klima')
            package['groups'].remove('Umwelt und Klima')

        if 'Verbraucherschutz' in package['groups']:
            package['groups'].append(u'verbraucher')
            package['groups'].remove('Verbraucherschutz')

        if 'Verwaltung' in package['groups']:
            package['groups'].append(u'verwaltung')
            package['groups'].remove('Verwaltung')
            package['groups'].remove('Haushalt und Steuern')

        if 'Wirtschaft und Arbeit' in package['groups']:
            package['groups'].append(u'wirtschaft_arbeit')
            package['groups'].remove('Wirtschaft und Arbeit')

        for cat in package['groups']:
            if 'Bev' in cat:
                package['groups'].append(u'bevoelkerung')

        from ckan.lib.munge import munge_title_to_name

        name = package['name']
        try:
            name = munge_title_to_name(name).replace('_', '-')
            while '--' in name:
                name = name.replace('--', '-')
        except Exception, e:
            log.debug('Encoding Error ' + str(e))

        package['name'] = name


class BonnCKANHarvester(KoelnCKANHarvester):
    '''
    A CKAN Harvester for Bonn.
    '''

    city = 'Bonn'

    def info(self):
        return {'name': 'bonn',
                'title': 'Bonn CKAN Harvester',
                'description': 'A CKAN Harvester for Bonn.'}


    def amend_package(self, package):
        super(BonnCKANHarvester, self).amend_package(package)

        if u'Öffentliche Verwaltung' in package['groups']:
            package['groups'].append('verwaltung')
	    package['groups'].remove(u'Öffentliche Verwaltung')
	if u'Haushalt und Steuern' in package['groups']:
            package['groups'].append('politik_wahlen')
            package['groups'].remove('Haushalt und Steuern')
	if u'Geographie' in package['groups']:
            package['groups'].append('geo')
            package['groups'].remove(u'Geographie')
            package['groups'].remove(u'Geologie und Geobasisdaten')
	if u'Politik und Wahlen' in package['groups']:
            package['groups'].append('politik_wahlen')
	    package['groups'].remove(u'Politik und Wahlen')
	if u'Bevölkerung' in package['groups']:
	    package['groups'].append('bevoelkerung')
	    package['groups'].remove(u'Bevölkerung')

	package['license_id'] = 'dl-de-by-1.0' if package['license_id'] == 'dl-de-by' else package['license_id']
	package['extras']['metadata_original_portal'] = 'http://opendata.bonn.de/'
