#!/usr/bin/python
# -*- coding: utf8 -*-

from ckanext.govdatade.harvesters.ckanharvester import DatahubCKANHarvester

import unittest


class DataHubIOHarvesterTest(unittest.TestCase):

    def test_package_valid(self):

        harvester = DatahubCKANHarvester()

        valid_package1 = 'hbz_unioncatalog'
        valid_package2 = 'lobid-resources'
        valid_package3 = 'deutsche-nationalbibliografie-dnb'
        valid_package4 = 'dnb-gemeinsame-normdatei'

        invalid_package1 = 'hbz_unioncatalog2'
        invalid_package2 = '_hbz_unioncatalog'
        invalid_package3 = 'xxx'

        self.assertTrue(harvester.package_valid(valid_package1))
        self.assertTrue(harvester.package_valid(valid_package2))
        self.assertTrue(harvester.package_valid(valid_package3))
        self.assertTrue(harvester.package_valid(valid_package4))

        self.assertFalse(harvester.package_valid(invalid_package1))
        self.assertFalse(harvester.package_valid(invalid_package2))
        self.assertFalse(harvester.package_valid(invalid_package3))

    def test_amend_package(self):

        harvester = DatahubCKANHarvester()
        package = {'name': 'Package Name',
                   'description': '   ',
                   'groups': ['bibliographic', 'lld', 'bibsoup'],
                   'resources': [],
                   'extras': {}}

        harvester.amend_package(package)
        portal = package['extras']['metadata_original_portal']
        self.assertEqual(portal, 'http://datahub.io/')
        self.assertEqual(package['groups'], ['bibliographic', 'lld', 'bibsoup', 'bildung_wissenschaft'])
        self.assertEqual(package['type'], 'datensatz')
