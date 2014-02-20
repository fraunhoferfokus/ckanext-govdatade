from ckan.logic import get_action
from ckanext.govdatade import CONFIG
from ckanext.govdatade.validators import link_checker
from ckanext.govdatade.validators import schema_checker
from datetime import datetime
from collections import defaultdict
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


def copy_report_asset_files():
    target_dir = CONFIG.get('validators', 'report_dir')
    target_dir = os.path.join(target_dir, 'assets')
    target_dir = os.path.abspath(target_dir)

    vendor_dir = os.path.dirname(__file__)
    vendor_dir = os.path.join(vendor_dir, '../../', 'lib/assets')
    vendor_dir = os.path.abspath(vendor_dir)

    distutils.dir_util.copy_tree(vendor_dir, target_dir, update=1)


def is_valid(source):
    try:
        value = json.loads(source)
        return (type(value) == dict or type(value) == list
                or isinstance(value, basestring))
    except ValueError:
        return False


def generate_link_checker_data(data):
    checker = link_checker.LinkChecker()
    redis = checker.redis_client
    num_metadata = eval(redis.get('general'))['num_datasets']

    data['linkchecker'] = {}
    data['portals'] = defaultdict(int)
    data['entries'] = defaultdict(list)

    for record in checker.get_records():
        if 'urls' not in record:
            continue

        for url, entry in record['urls'].iteritems():
            if type(entry['status']) == int:
                entry['status'] = 'HTTP %s' % entry['status']

        if 'metadata_original_portal' in record:  # legacy
            portal = record['metadata_original_portal']
            data['portals'][portal] += 1
            data['entries'][portal].append(record)

    lc_stats = data['linkchecker']
    lc_stats['broken'] = sum(data['portals'].values())
    lc_stats['working'] = num_metadata - lc_stats['broken']


def generate_schema_checker_data(data):
    validator = schema_checker.SchemaChecker()
    redis = validator.redis_client
    num_metadata = eval(redis.get('general'))['num_datasets']

    data['schema']['portal_statistic'] = defaultdict(int)
    data['schema']['rule_statistic'] = defaultdict(int)
    data['schema']['broken_rules'] = defaultdict(defaultdict)

    portals = data['schema']['portal_statistic']
    rules = data['schema']['rule_statistic']
    broken_rules = data['schema']['broken_rules']

    broken = 0

    for record in validator.get_records():
        dataset_id = record['id']
        portal = record['metadata_original_portal']

        portals[portal] += 1

        if 'schema' not in record:
            continue

        if record['schema']:
            broken += 1

        broken_rules[portal][dataset_id] = record['schema']
        for broken_rule in record['schema']:
            rules[broken_rule[0]] += 1

    sc_stats = data['schemachecker']
    sc_stats['broken'] = broken
    sc_stats['working'] = num_metadata - sc_stats['broken']


def generate_general_data(data):
    validator = schema_checker.SchemaChecker()
    redis = validator.redis_client

    data['num_datasets'] = eval(redis.get('general'))['num_datasets']
    data['timestamp'] = datetime.today().strftime("%Y-%m-%d %H:%M")


def amend_portal(portal):
    portal = portal.replace(':', '-')
    portal = portal.replace('/', '-')
    portal = portal.replace('.', '-')

    portal = portal.replace('&', '-')
    portal = portal.replace('?', '-')
    portal = portal.replace('=', '-')

    return portal

