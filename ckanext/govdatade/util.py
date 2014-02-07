from ckanext.govdatade import CONFIG
from ckan.logic import get_action
from math import ceil

import ckanclient
import distutils.dir_util
import json
import os


def iterate_remote_datasets(endpoint, max_rows=1000):
    ckan = ckanclient.CkanClient(base_location=endpoint)

    print 'Retrieve total number of datasets'
    total = ckan.action('package_search', rows=1)['count']

    steps = int(ceil(total / float(max_rows)))
    rows = max_rows

    for i in range(0, steps):
        if i == steps - 1:
            rows = total - (i * rows)

        datasets = (i * 1000) + 1
        print 'Retrieve datasets %s - %s' % (datasets, datasets + rows - 1)

        records = ckan.action('package_search', rows=rows, start=rows * i)
        records = records['results']
        for record in records:
            yield record


def iterate_local_datasets(context, rows=1000):
    package_list = get_action('package_list')
    package_show = get_action('package_show')

    for dataset_name in package_list(context, None):
        dataset = package_show(context, {'id': dataset_name})
        yield dataset


def normalize_action_dataset(dataset):
    dataset['groups'] = [group['name'] for group in dataset['groups']]
    dataset['tags'] = [group['name'] for group in dataset['tags']]

    extras = {}
    for entry in dataset['extras']:
        extras[entry['key']] = entry['value']

    dataset['extras'] = normalize_extras(extras)


def normalize_extras(source):
    if type(source) == dict:
        result = {}
        for key, value in source.iteritems():
            result[key] = normalize_extras(value)
        return result
    elif type(source) == list:
        return [normalize_extras(item) for item in source]
    elif isinstance(source, basestring) and is_valid(source):
        return normalize_extras(json.loads(source))
    else:
        return source


def copy_report_vendor_files():
    target_dir = CONFIG.get('validators', 'report_dir')
    target_dir = os.path.join(target_dir, 'assets')
    target_dir = os.path.abspath(target_dir)

    vendor_dir = os.path.dirname(__file__)
    vendor_dir = os.path.join(vendor_dir, '../../', 'lib/vendor')
    vendor_dir = os.path.abspath(vendor_dir)

    distutils.dir_util.copy_tree(vendor_dir, target_dir, update=1)


def is_valid(source):
    try:
        value = json.loads(source)
        return (type(value) == dict or type(value) == list
                or isinstance(value, basestring))
    except ValueError:
        return False
