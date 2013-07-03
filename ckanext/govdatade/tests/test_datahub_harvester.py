#!/usr/bin/python
# -*- coding: utf8 -*-

from ckanext.govdatade.harvesters.ckanharvester import DatahubCKANHarvester

import unittest


class DataHubIOHarvesterTest(unittest.TestCase):

    def test_harvest_package(self):

        harvester = DatahubCKANHarvester()

        valid_package1 = 'hbz_unioncatalog'
        valid_package2 = 'lobid-resources'
        valid_package3 = 'deutsche-nationalbibliografie-dnb'
        valid_package4 = 'dnb-gemeinsame-normdatei'

        invalid_package1 = 'hbz_unioncatalog2'
        invalid_package2 = '_hbz_unioncatalog'
        invalid_package3 = 'xxx'

        self.assertTrue(harvester.harvest_package(valid_package1))
        self.assertTrue(harvester.harvest_package(valid_package2))
        self.assertTrue(harvester.harvest_package(valid_package3))
        self.assertTrue(harvester.harvest_package(valid_package4))

        self.assertFalse(harvester.harvest_package(invalid_package1))
        self.assertFalse(harvester.harvest_package(invalid_package2))
        self.assertFalse(harvester.harvest_package(invalid_package3))
