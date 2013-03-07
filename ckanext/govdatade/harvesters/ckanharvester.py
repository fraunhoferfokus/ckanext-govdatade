import uuid
from ckan.lib.helpers import json
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester
from ckanext.harvest.model import HarvestObject
import urllib2


class HamburgCKANHarvester(CKANHarvester):
    '''
    A CKAN Harvester for Hamburg solving data compatibility problems.
    '''

    api_version = 1
    '''Enforce API version 1 for enabling group import'''

    def info(self):
        return {'name':        'hamburg',
                'title':       'Hamburg Harvester',
                'description': 'A CKAN Harvester for Hamburg solving data compatibility problems.'}

    def _set_config(self, config_str):
        '''Enforce API version 1 for enabling group import'''
        self.api_version = 1
        self.config = {'api_version': '1'}

    def import_stage(self, harvest_object):
        package_dict = json.loads(harvest_object.content)
        package_dict['groups'] = [name.replace('-', '_') for name in package_dict['groups']]
        harvest_object.content = json.dumps(package_dict)
        return super(HamburgCKANHarvester, self).import_stage(harvest_object)
    
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
        return super(BremenCKANHarvester, self).import_stage(harvest_object)

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
        return super(BayernCKANHarvester, self).import_stage(harvest_object)
        
        
        