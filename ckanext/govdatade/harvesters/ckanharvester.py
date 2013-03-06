from ckan.lib.helpers import json
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester

import logging


AUTHOR = 'author'
AUTHOR_EMAIL = 'author_email'
MAINTAINER = 'maintainer'
MAINTAINER_EMAIL = 'maintainer_email'
GROUPS = 'groups'
TAGS = 'tags'

log = logging.getLogger(__name__)


def assert_author_fields(package_dict):
    """Ensures that the author field is set."""

    if not package_dict[AUTHOR]:
        package_dict[AUTHOR] = package_dict[MAINTAINER]

    if not package_dict[AUTHOR_EMAIL]:
        package_dict[AUTHOR_EMAIL] = package_dict[MAINTAINER_EMAIL]

    if not package_dict[AUTHOR]:
        raise Exception('There is no author/maintainer for package %s' % package_dict['id'])


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
        assert_author_fields(package_dict)

        harvest_object.content = json.dumps(package_dict)
        return super(HamburgCKANHarvester, self).import_stage(harvest_object)
