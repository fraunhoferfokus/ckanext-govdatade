#!/usr/bin/python
# -*- coding: utf8 -*-

from jsonschema import validate, ValidationError
from nose.tools import raises

import json
import urllib2


class TestValidation:

    SCHEMA_URL = "https://raw.github.com/fraunhoferfokus/ogd-metadata/master/OGPD_JSON_Schema.json"  # NOQA

    def setup(self):
        self.schema = json.loads(urllib2.urlopen(self.SCHEMA_URL).read())

    @raises(ValidationError)
    def test_empty_package(self):
        validate({}, self.schema)
