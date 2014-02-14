from jsonschema.validators import Draft3Validator

import json
import urllib2
import redis


class SchemaChecker:

    SCHEMA_URL = 'https://raw.github.com/fraunhoferfokus/ogd-metadata/master/OGPD_JSON_Schema.json'  # NOQA

    def __init__(self, db='production'):
        redis_db_dict = {'production': 0, 'test': 1}
        database = redis_db_dict[db]
        self.schema = json.loads(urllib2.urlopen(self.SCHEMA_URL).read())
        self.redis_client = redis.StrictRedis(host='localhost',
                                              port=6379,
                                              db=database)

    def process_record(self, dataset):
        dataset_id = dataset['id']
        record = self.redis_client.get(dataset_id)

        portal = dataset['extras'].get('metadata_original_portal', 'null')
        portal = portal.replace('http://', '')
        portal = portal.replace('/', '')

        if record is None:
            record = {'id': dataset_id, 'metadata_original_portal': portal}
        else:
            record = eval(record)

        record['schema'] = []
        broken_rules = []

        errors = Draft3Validator(self.schema).iter_errors(dataset)

        if not Draft3Validator(self.schema).is_valid(dataset):
            errors = Draft3Validator(self.schema).iter_errors(dataset)

            for error in errors:
                path = [e for e in error.path if isinstance(e, basestring)]
                path = str('.'.join(map((lambda e: str(e)), path)))

                field_path_message = [path, error.message]
                broken_rules.append(field_path_message)

        record['schema'] = broken_rules
        self.redis_client.set(dataset_id, record)
