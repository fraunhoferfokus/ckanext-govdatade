import uuid
from ckan.lib.helpers import json
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.harvest.model import HarvestObject
import urllib2
from ckanext.govdatade.harvesters.translator import translate_groups

import ConfigParser
import logging


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
        config = ConfigParser.RawConfigParser()
        config.read('config.ini')
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

class JSONDumpBaseCKANHarvester(CKANHarvester):
    
    def info(self):
        return {'name':        'base',
                'title':       'Base Harvester',
                'description': 'A Base CKAN Harvester for CKANs which return a JSON dump file.'}
        
    def _set_config(self, config_str):
        '''Enforce API version 1 for enabling group import'''
        self.api_version = 1
        self.config = {'api_version': '1'}
       
    def gather_stage(self, harvest_job):
        self._set_config(harvest_job.source.config)
        # Request all remote packages
        try:
            content = self._get_content(harvest_job.source.url)
        except Exception,e:
            self._save_gather_error('Unable to get content for URL: %s: %s' % (harvest_job.source.url, str(e)),harvest_job)
            return None

        object_ids = []
        
        packages = json.loads(content)
        for package in packages:
            obj = HarvestObject(guid = package['name'], job = harvest_job)
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
    bremen_to_germany_url = "https://github.com/fraunhoferfokus/ogd-metadata/raw/master/kategorien/bremen2deutschland.json"
    translation_map = json.loads( urllib2.urlopen(bremen_to_germany_url).read())  
    
    def info(self):
        return {'name':        'bremen',
                'title':       'Bremen CKAN Harvester',
                'description': 'A CKAN Harvester for Bremen.'}
    
        
    def import_stage(self, harvest_object):
        '''
        This function fixes some differences in the datasets retrieved from Bremen and our schema such as:
        - fix groups
        - set metadata_original_portal
        - fix terms_of_use
        - copy veroeffentlichende_stelle to maintainer
        - set spatial text
        '''
        package = json.loads(harvest_object.content)
            
        #set original portal
        package['extras']['metadata_original_portal'] = 'http://daten.bremen.de/sixcms/detail.php?template=export_daten_json_d' 

        # set correct groups
        if not package['groups']:
            package['groups'] = []
        package['groups'] = [value for group in package['groups'] for value in self.translation_map[group]]
        #copy veroeffentlichende_stelle to maintainer
        quelle = filter(lambda x: x['role'] == 'veroeffentlichende_stelle', package['extras']['contacts'])[0]
        package['maintainer'] = quelle['name']
        package['maintainer_email'] = quelle['email']
        
        #fix typos in terms of use
        self.fix_terms_of_use(package['extras']['terms_of_use'])
        #copy license id
        package['license_id'] = package['extras']['terms_of_use']['license_id']
        
        
        if not "spatial-text" in package["extras"].keys():
            package["extras"]["spatial-text"] = 'Bremen 04 0 11 000'
        
        #generate id based on OID namespace and package name, this makes sure, 
        #that packages with the same name get the same id
        package['id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))
        
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
    
        
    def import_stage(self, harvest_object):
        package = json.loads(harvest_object.content)
        
        if not package['groups']:
            package['groups'] = []
        # there is no mapping between bavarian groups and german groups
        # package['groups'] = package['groups'] + transl.translate(package['groups'])
        #copy ansprechpartner to author
        quelle = filter(lambda x: x['role'] == 'ansprechpartner', package['extras']['contacts'])[0]
        package['author'] = quelle['name']
        if 'email' in quelle.keys():
            package['author_email'] = quelle['email']
        
        #copy veroeffentlichende_stelle to maintainer
        quelle = filter(lambda x: x['role'] == 'veroeffentlichende_stelle', package['extras']['contacts'])[0]
        package['maintainer'] = quelle['name']
        package['maintainer_email'] = quelle['email']
        package['license_id'] = package['extras']['terms_of_use']['license_id']
        if not "spatial-text" in package["extras"].keys():
            package["extras"]["spatial-text"] = 'Bayern 09'
        for r in package['resources']:
            r['format'] = r['format'].upper()
            
        
        #generate id based on OID namespace and package name, this makes sure, 
        #that packages with the same name get the same id
        package['id'] = str(uuid.uuid5(uuid.NAMESPACE_OID, str(package['name'])))

        harvest_object.content = json.dumps(package)
        super(BayernCKANHarvester, self).import_stage(harvest_object)
