#!/bin/env python
from ckanext.govdatade import CONFIG

import json
import os
import urllib2


def translate_groups(groups, source_name):
    url = CONFIG.get('URLs', 'categories') + source_name + '2deutschland.json'
    json_string = urllib2.urlopen(url).read()
    group_dict = json.loads(json_string)

    result = []
    for group in groups:
        if group in group_dict:
            result = result + group_dict[group]
    return result
