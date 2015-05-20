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

        if len(self.args) > 0:
            fq = self.args[0]
        else:
            fq = None

        package_search = tk.get_action('package_search')
        package_delete = tk.get_action('package_delete')
        context = {'model': model, 'session': model.Session, 'ignore_auth': True}
        self.admin_user = tk.get_action('get_site_user')(context, {})
        context = {'model': model, 'session': model.Session, 'user': self.admin_user['name']}

        search_params = {'rows': 100000}
        if fq:
            search_params['fq'] = fq

        result = package_search(context, search_params)
        result = result['results']

        for r in result:
            print r['title'][0:40] + '... - ' + r['name']

        print '============================================================='
        print str(len(result)) + ' Datasets found.'
        input = raw_input("Type in 'delete' for deleting all shown datasets. Type 'exit' for quitting. [delete/exit] ")

        success = 0
        error = 0
        if input == 'delete':
            for r in result:
                try:
                    delete_result = package_delete(context, {'id': r['id']})
                    print '%s deleted' % r['name']
                    self._purge(r['id'])
                    success += 1
                except Exception as e:
                    print 'ERROR: ' + e
                    error += 1

        print '============================================================='
        print 'Successfully deleted and purged %d Datasets' % success
        print '%d Datasets could not be deleted and/or purged' % error

    def _get_dataset(self, dataset_ref):
        import ckan.model as model
        dataset = model.Package.get(unicode(dataset_ref))
        assert dataset, 'Could not find dataset matching reference: %r' % dataset_ref
        return dataset

    def _purge(self, dataset_ref):
        from ckan import plugins
        import ckan.model as model
        dataset = self._get_dataset(dataset_ref)
        name = dataset.name

        plugins.load('synchronous_search')
        rev = model.repo.new_revision()
        dataset.purge()
        model.repo.commit_and_remove()
        print '%s purged' % name
