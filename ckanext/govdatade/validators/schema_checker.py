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

	try:
	   record = eval(record)
	except:
	   print "EVAL_ERROR"

	print "RECORD: ", record

        portal = dataset['extras'].get('metadata_original_portal', 'null')

        if record is None:
            record = {'id': dataset_id, 'metadata_original_portal': portal}
        else:
	    try:                  
               record = eval(record)
            except:
               print "Record_error: ", record
	       print "rec_Schema___:", record['schema']
        try:
           record['schema'] = []
        except:
           print "TypeError"
        
        broken_rules = []

        errors = Draft3Validator(self.schema).iter_errors(dataset)

        if not Draft3Validator(self.schema).is_valid(dataset):
            errors = Draft3Validator(self.schema).iter_errors(dataset)

            for error in errors:
                path = [e for e in error.path if isinstance(e, basestring)]
                path = str('.'.join(map((lambda e: str(e)), path)))

                field_path_message = [path, error.message]
                broken_rules.append(field_path_message)

        dataset_groups = dataset['groups']
        
        if (len(dataset_groups) >= 4):
            path = "groups"
            field_path_message = [path, "WARNING: too many groups set"]
            broken_rules.append(field_path_message)

	print "broken_rules: ", broken_rules

	try:
           record['schema'] = broken_rules
	except:
	   print "broken_rules_Error: ", broken_rules

        self.redis_client.set(dataset_id, record)

        return not broken_rules

    def get_records(self):
        result = []
        for dataset_id in self.redis_client.keys('*'):
            if dataset_id == 'general':
                continue
	    try:
               result.append(eval(self.redis_client.get(dataset_id)))
	    except:
	       print "DS_errer_schema: ", dataset_id

        return result
