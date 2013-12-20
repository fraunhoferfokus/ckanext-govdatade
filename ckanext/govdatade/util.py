from math import ceil

import ckanclient
import json


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


def normalize_action_dataset(dataset):
    dataset['groups'] = [group['name'] for group in dataset['groups']]
    dataset['tags'] = [group['name'] for group in dataset['tags']]
    dataset['extras'] = {e['key']: e['value'] for e in dataset['extras']}
    dataset['extras'] = normalize_extras(dataset['extras'])


def normalize_extras(source):
    if type(source) == dict:
        return {key: normalize_extras(value) for key, value in source.items()}
    elif type(source) == list:
        return [normalize_extras(item) for item in source]
    elif isinstance(source, basestring) and is_valid(source):
        return normalize_extras(json.loads(source))
    else:
        return source


def is_valid(source):
    try:
        value = json.loads(source)
        return (type(value) == dict or type(value) == list
                or isinstance(value, basestring))
    except ValueError:
        return False
