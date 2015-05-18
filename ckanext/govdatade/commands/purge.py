__author__ = 'fki'

from ckan.lib.cli import CkanCommand
import ckan.plugins.toolkit as tk
from ckan.lib.base import model


class Purge(CkanCommand):
    '''Purges datasets by a given attribute.'''

    summary = __doc__.split('\n')[0]

    def __init__(self, name):
        super(Purge, self).__init__(name)

    def command(self):
        self._load_config()

        package_search = tk.get_action('package_search')
        package_delete = tk.get_action('package_delete')
        context = {'model': model, 'session': model.Session, 'ignore_auth': True}
        self.admin_user = tk.get_action('get_site_user')(context, {})
        context = {'model': model, 'session': model.Session, 'user': self.admin_user['name']}

        # result = package_search(context, {})
        # for r in result['results']:
        #     print r['title']

        package_delete(context, {'id': 'advmis_debkg00m00000100'})

        print "Hallo Purge"