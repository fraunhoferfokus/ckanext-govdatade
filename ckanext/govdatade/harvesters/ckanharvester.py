from ckan.lib.helpers import json
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester


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
        if config_str:
            self.config = json.loads(config_str)
            if 'api_version' in self.config:
                self.api_version = self.config['api_version']
        else:
            self.api_version = 1
            self.config = {'api_version': '1'}

    def import_stage(self, harvest_object):
        package_dict = json.loads(harvest_object.content)
        package_dict['groups'] = [name.replace('-', '_') for name in package_dict['groups']]
        harvest_object.content = json.dumps(package_dict)
        return super(HamburgCKANHarvester, self).import_stage(harvest_object)
