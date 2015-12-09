#!/usr/bin/python
# -*- coding: utf8 -*-

from ckanext.harvest.model import HarvestObject
from ckanext.govdatade.harvesters.translator import translate_groups
from ckanext.govdatade.harvesters.ckanharvester import GovDataHarvester

import json
import logging
import uuid
import zipfile
import StringIO
import httplib
from urlparse import urlparse

log = logging.getLogger(__name__)


class JSONDumpBaseCKANHarvester(GovDataHarvester):
    
    def info(self):
        return {'name': 'base',
                'title': 'Base Harvester',
                'description': 'A Base CKAN Harvester for CKANs which return a JSON dump file.'}

    def get_remote_dataset_names(self, content):
        log.info('Calling JSONDumpBaseCKANHarvester get_remote_dataset_names..')
        remote_dataset_names = json.loads(content)
        remote_dataset_names = map(lambda d: d.get('name'), remote_datasets)
        log.info('REMOTE_DATASET_NAMES: ' + str(remote_dataset_names))
        return remote_dataset_names
    
    def gather_stage(self, harvest_job):
        self._set_config(harvest_job.source.config)
        # Request all remote packages
        try:
            content = self._get_content(harvest_job.source.url)
        except Exception, e:
            self._save_gather_error('Unable to get content for URL: %s: %s' % (harvest_job.source.url, str(e)),
                                    harvest_job)
            return None

        object_ids = []

        packages = json.loads(content)
        for package in packages:
            obj = HarvestObject(guid=package['name'], job=harvest_job)
            obj.content = json.dumps(package)
            obj.save()
            object_ids.append(obj.id)

        context = self.build_context()
        remote_dataset_names = self.get_remote_dataset_names(content)
        self.delete_deprecated_datasets(context, remote_dataset_names)

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
    """
    A CKAN Harvester for Bremen. The Harvester retrieves a JSON dump,
    which will be loaded to CKAN.
    """

    PORTAL = 'http://daten.bremen.de/sixcms/detail.php?template=export_daten_json_d'

    def info(self):
        return {'name': 'bremen',
                'title': 'Bremen CKAN Harvester',
                'description': 'A CKAN Harvester for Bremen.'}

    def amend_package(self, package):
        """
        This function fixes some differences in the datasets retrieved from Bremen and our schema such as:
        - fix groups
        - set metadata_original_portal
        - fix terms_of_use
        - copy veroeffentlichende_stelle to maintainer
        - set spatial text
        """

        # set metadata original portal
        package['extras'][
            'metadata_original_portal'] = self.PORTAL

        # set correct groups
        if not package['groups']:
            package['groups'] = []
        package['groups'] = translate_groups(package['groups'], 'bremen')

        # copy veroeffentlichende_stelle to maintainer
        if 'contacts' in package['extras']:
            quelle = filter(lambda x: x['role'] == 'veroeffentlichende_stelle', package['extras']['contacts'])[0]
            package['maintainer'] = quelle['name']
            package['maintainer_email'] = quelle['email']

        # fix typos in terms of use
        if 'terms_of_use' in package['extras']:
            self.fix_terms_of_use(package['extras']['terms_of_use'])
            # copy license id
            package['license_id'] = package['extras']['terms_of_use']['license_id']
        else:
            package['license_id'] = u'notspecified'

        if "spatial-text" not in package["extras"]:
            package["extras"]["spatial-text"] = 'Bremen 04 0 11 000'

        # generate id based on OID namespace and package name, this makes sure,
        # that packages with the same name get the same id
        package['id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))
        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(BremenCKANHarvester, self).import_stage(harvest_object)

    def fix_terms_of_use(self, terms_of_use):
        terms_of_use['license_id'] = terms_of_use['licence_id']
        del (terms_of_use['licence_id'])
        terms_of_use['license_url'] = terms_of_use['licence_url']
        del (terms_of_use['licence_url'])


class BayernCKANHarvester(JSONDumpBaseCKANHarvester):
    """
    A CKAN Harvester for Bavaria. The Harvester retrieves a JSON dump,
    which will be loaded to CKAN.
    """

    def info(self):
        return {'name': 'bayern',
                'title': 'Bavarian CKAN Harvester',
                'description': 'A CKAN Harvester for Bavaria.'}

    def amend_package(self, package):
        if len(package['name']) > 100:
            package['name'] = package['name'][:100]
        if not package['groups']:
            package['groups'] = []

        # copy autor to author
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

        # generate id based on OID namespace and package name, this makes sure,
        # that packages with the same name get the same id
        package['id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))
        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(BayernCKANHarvester, self).import_stage(harvest_object)


class MoersCKANHarvester(JSONDumpBaseCKANHarvester):
    """A CKAN Harvester for Moers solving data compatibility problems."""

    PORTAL = 'http://www.offenedaten.moers.de/'

    def info(self):
        return {'name': 'moers',
                'title': 'Moers Harvester',
                'description': 'A CKAN Harvester for Moers solving data compatibility problems.'}

    def amend_dataset_name(self, dataset):
        dataset['name'] = dataset['name'].replace(u'Ã¤', 'ae')
        dataset['name'] = dataset['name'].replace(u'Ã¼', 'ue')
        dataset['name'] = dataset['name'].replace(u'Ã¶', 'oe')

        dataset['name'] = dataset['name'].replace('(', '')
        dataset['name'] = dataset['name'].replace(')', '')
        dataset['name'] = dataset['name'].replace('.', '')
        dataset['name'] = dataset['name'].replace('/', '')
        dataset['name'] = dataset['name'].replace('http://www.moers.de', '')

    def amend_package(self, package):

        publishers = filter(lambda x: x['role'] == 'veroeffentlichende_stelle', package['extras']['contacts'])
        maintainers = filter(lambda x: x['role'] == 'ansprechpartner', package['extras']['contacts'])

        if not publishers:
            raise ValueError('There is no author email for package %s' % package['id'])

        self.amend_dataset_name(package)
        package['id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))
        package['name'] = package['name'].lower()

        if 'moers' not in package['title'].lower():
            package['title'] += ' Moers'

        package['author'] = 'Stadt Moers'
        package['author_email'] = publishers[0]['email']

        if maintainers:
            package['maintainer'] = maintainers[0]['name']
            package['maintainer_email'] = maintainers[0]['email']

        package['license_id'] = package['extras']['terms_of_use']['license_id']
        package['extras']['metadata_original_portal'] = self.PORTAL

        if not "spatial-text" in package["extras"].keys():
            package["extras"]["spatial-text"] = '05 1 70 024 Moers'

        if isinstance(package['tags'], basestring):
            if not package['tags']:  # if tags was set to "" or null
                package['tags'] = []
            else:
                package['tags'] = [package['tags']]
        package['tags'].append('moers')

        for resource in package['resources']:
            resource['format'] = resource['format'].replace('text/comma-separated-values', 'xls')
            resource['format'] = resource['format'].replace('application/json', 'json')
            resource['format'] = resource['format'].replace('application/xml', 'xml')

        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

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
    """
    A CKAN Harvester for GovApps. The Harvester retrieves a JSON dump,
    which will be loaded to CKAN.
    """

    def info(self):
        return {'name': 'govapps',
                'title': 'GovApps Harvester',
                'description': 'A CKAN Harvester for GovApps.'}

    def amend_package(self, package):
        if not package['groups']:
            package['groups'] = []
        # fix groups
        if not package['groups']:
            package['groups'] = []
        package['groups'] = [x for x in translate_groups(package['groups'], 'govapps') if len(x) > 0]

        # generate id based on OID namespace and package name, this makes sure,
        # that packages with the same name get the same id
        package['id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))

        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(GovAppsHarvester, self).import_stage(harvest_object)


class JSONZipBaseHarvester(JSONDumpBaseCKANHarvester):
    
    def info(self):
        return {'name': 'zipbase',
                'title': 'Base Zip Harvester',
                'description': 'A Harvester for Portals, which return JSON files in a zip file.'}

    def gather_stage(self, harvest_job):
        self._set_config(harvest_job.source.config)
        # Request all remote packages
        try:
            content = self._get_content(harvest_job.source.url)
        except Exception, e:
            self._save_gather_error('Unable to get content for URL: %s: %s' % (harvest_job.source.url, str(e)),
                                    harvest_job)
            return None

        object_ids = []
        packages = []
        file_content = StringIO.StringIO(content)
        archive = zipfile.ZipFile(file_content, "r")
        remote_dataset_names = []
        for name in archive.namelist():
            if name.endswith(".json"):
                package = json.loads(archive.read(name))
                packages.append(package)
                remote_dataset_names.append(package['name'])
                obj = HarvestObject(guid=package['name'], job=harvest_job)
                obj.content = json.dumps(package)
                obj.save()
                object_ids.append(obj.id)
                
        log.info('REMOTE_DATASET_NAMES: ' + str(remote_dataset_names))
        context = self.build_context()
        self.delete_deprecated_datasets(context, remote_dataset_names)
        
        if object_ids:
            return object_ids
        else:
            self._save_gather_error('No packages received for URL: %s' % harvest_job.source.url,
                                    harvest_job)
            return None


class BKGHarvester(JSONZipBaseHarvester):
    PORTAL = 'http://ims.geoportal.de/'

    def info(self):
        return {'name': 'bkg',
                'title': 'BKG CKAN Harvester',
                'description': 'A CKAN Harvester for BKG.'}

    def amend_package(self, package):
        # generate id based on OID namespace and package name, this makes sure,
        # that packages with the same name get the same id
        package['id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))
        package['extras']['metadata_original_portal'] = self.PORTAL
        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(JSONZipBaseHarvester, self).import_stage(harvest_object)


class DestatisZipHarvester(JSONZipBaseHarvester):
    PORTAL = 'http://www-genesis.destatis.de/'

    def info(self):
        return {'name': 'destatis',
                'title': 'Destatis CKAN Harvester',
                'description': 'A CKAN Harvester for destatis.'}

    def amend_package(self, package):
        # generate id based on OID namespace and package name, this makes sure,
        # that packages with the same name get the same id

        package['id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))
        package['extras']['metadata_original_portal'] = self.PORTAL

        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(JSONZipBaseHarvester, self).import_stage(harvest_object)


class RegionalStatistikZipHarvester(JSONZipBaseHarvester):

    PORTAL = 'https://www.regionalstatistik.de/'

    def info(self):
        return {'name': 'regionalStatistik',
                'title': 'RegionalStatistik CKAN Harvester',
                'description': 'A CKAN Harvester for Regional Statistik.'}

    def amend_package(self, package):
        # generate id based on OID namespace and package name, this makes sure,
        # that packages with the same name get the same id
        package['id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))
        package['extras']['metadata_original_portal'] = self.PORTAL
        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)
        self.amend_package(package)
        harvest_object.content = json.dumps(package)
        super(JSONZipBaseHarvester, self).import_stage(harvest_object)


class SecondDestatisZipHarvester(JSONZipBaseHarvester):
    PORTAL = 'http://destatis.de/'

    def info(self):
        return {'name': 'destatis2',
                'title': 'Destatis CKAN Harvester',
                'description': 'A CKAN Harvester for destatis.'}

    def amend_package(self, package):
        # generate id based on OID namespace and package name, this makes sure,
        # that packages with the same name get the same id

        package['id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))
        package['extras']['metadata_original_portal'] = self.PORTAL

        for resource in package['resources']:
            resource['format'] = resource['format'].lower()

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(JSONZipBaseHarvester, self).import_stage(harvest_object)
    
    def gather_stage(self, harvest_job):
        self._set_config(harvest_job.source.config)
        # Request all remote packages
        try:
            content = self._get_content(harvest_job.source.url)
        except Exception, e:
            self._save_gather_error('Unable to get content for URL: %s: %s' % (harvest_job.source.url, str(e)),
                                    harvest_job)
            return None

        object_ids = []
        packages = []

        file_content = StringIO.StringIO(content)
        archive = zipfile.ZipFile(file_content, "r")
        for name in archive.namelist():
            if name.endswith(".json"):
                _input = archive.read(name)
		_input = _input.decode("utf-8-sig")
		package = json.loads(_input)
                packages.append(package)
                obj = HarvestObject(guid=package['name'], job=harvest_job)
                obj.content = json.dumps(package)
                obj.save()
                object_ids.append(obj.id)

        remote_dataset_names = self.get_remote_dataset_names(content)
        context = self.build_context()
        self.delete_deprecated_datasets(context, remote_dataset_names)
        if object_ids:
            return object_ids
        else:
            self._save_gather_error('No packages received for URL: %s' % harvest_job.source.url,
                                    harvest_job)
            return None


class SachsenZipHarvester(SecondDestatisZipHarvester):
    
    PORTAL = 'http://www.statistik.sachsen.de/'

    def info(self):
        return {'name': 'sachsen',
                'title': 'Sachsen Harvester',
                'description': 'A CKAN Harvester for Sachsen.'}

    def amend_package(self, package):
        # generate id based on OID namespace and package name, this makes sure,
        # that packages with the same name get the same id
        package['id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))
        package['extras']['metadata_original_portal'] = self.PORTAL

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)
        self.amend_package(package)
        harvest_object.content = json.dumps(package)
        super(JSONZipBaseHarvester, self).import_stage(harvest_object)
        
class BMBF_ZipHarvester(JSONDumpBaseCKANHarvester):

    PORTAL = 'http://www.datenportal.bmbf.de/'

    def info(self):
        return {'name': 'bmbf',
                'title': 'BMBF JSON zip Harvester',
                'description': 'A JSON zip Harvester for BMBF.'}
        
     
     
    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
        else:
            self.config = {}
        self.api_version = 1
        self.config['api_version'] = 1
        self.config['force_all'] = True
        self.config['remote_groups'] = 'only_local'
        self.config['user'] = 'bmbf-datenportal'
        
        
    def amend_package(self, package):
        
        package['id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))
        package['extras']['metadata_original_portal'] = self.PORTAL
        
        for resource in package['resources']:
            resource['format'] = resource['format'].lower()
             
                
    def import_stage(self, harvest_object):
        
        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(BMBF_ZipHarvester, self).import_stage(harvest_object)


class BfJHarvester(JSONZipBaseHarvester):

    PORTAL = 'https://www.bundesjustizamt.de'

    def info(self):
        return {'name': 'bfj',
                'title': 'BfJ CKAN Harvester',
                'description': 'A CKAN Harvester for BfJ.'}

    def amend_package(self, package):
        # generate id based on OID namespace and package name, this makes sure,
        # that packages with the same name get the same id
        package['id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))
        package['extras']['metadata_original_portal'] = self.PORTAL
        for resource in package['resources']:
            resource['format'] = resource['format'].lower()
            
    def gather_stage(self, harvest_job):

        self._set_config(harvest_job.source.config)
        # Request all remote packages
        try:
            #split the url into base, path and query to set up a https connectino before downloading the *.zip
            #url has to start with 'https://..' 
            parsed_uri = urlparse(harvest_job.source.url)
            domain = '{uri.netloc}'.format(uri=parsed_uri)
            url = '{uri.path}?{uri.query}'.format(uri=parsed_uri) 
            
            conn = httplib.HTTPSConnection(domain)
            conn.request("GET", url)
            response = conn.getresponse()
            content = response.read()
        except Exception, e:
            self._save_gather_error('Unable to get content for URL: %s: %s' % (harvest_job.source.url, str(e)),
                                    harvest_job)
            return None

        object_ids = []
        packages = []

        file_content = StringIO.StringIO(content)
        archive = zipfile.ZipFile(file_content, "r")
        
        for name in archive.namelist():
            if name.endswith(".json"):
                _input = archive.read(name)
                _input = _input.decode("latin9")
                package = json.loads(_input)
                packages.append(package)
                obj = HarvestObject(guid=package['name'], job=harvest_job)
                obj.content = json.dumps(package)
                obj.save()
                object_ids.append(obj.id)

        remote_dataset_names = self.get_remote_dataset_names(content)
        context = self.build_context()
        self.delete_deprecated_datasets(context, remote_dataset_names)
        if object_ids:
            return object_ids
        else:
            self._save_gather_error('No packages received for URL: %s' % harvest_job.source.url,
                                    harvest_job)
            return None
        
        

    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)

        self.amend_package(package)

        harvest_object.content = json.dumps(package)
        super(JSONZipBaseHarvester, self).import_stage(harvest_object)

