#!/bin/env python
import ConfigParser
import json
import os
import urllib2


def translate_groups(groups, source_name):
    config = ConfigParser.RawConfigParser()

    config_file = os.path.dirname(__file__)
    config_file = os.path.join(config_file, '../../..', 'config.ini')
    config_file = os.path.abspath(config_file)

    config.read(config_file)

    url = config.get('URLs', 'categories') + source_name + '2deutschland.json'
    json_string = urllib2.urlopen(url).read()
    group_dict = json.loads(json_string)

    result = []
    for group in groups:
        if group in group_dict:
            result = result + group_dict[group]
    return result
