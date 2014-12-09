#!/usr/bin/python
# -*- coding: utf8 -*-

from ckanext.govdatade.harvesters.ckanharvester import BerlinCKANHarvester
from ckanext.govdatade.harvesters.ckanharvester import MoersCKANHarvester
from ckanext.govdatade.harvesters.ckanharvester import RLPCKANHarvester

import os
import json
import unittest


class BerlinHarvesterTest(unittest.TestCase):

    def test_sector_amendment(self):

        harvester = BerlinCKANHarvester()

        dataset = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None}}

        valid = harvester.amend_package(dataset)
        self.assertEqual(dataset['extras']['sector'], 'oeffentlich')
        self.assertTrue(valid)

        dataset = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None,
                              'sector':                   None}}

        valid = harvester.amend_package(dataset)
        self.assertEqual(dataset['extras']['sector'], 'oeffentlich')
        self.assertTrue(valid)

        dataset = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None,
                              'sector':                   'privat'}}

        valid = harvester.amend_package(dataset)
        self.assertEqual(dataset['extras']['sector'], 'privat')
        self.assertFalse(valid)

        dataset = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None,
                              'sector':                   'andere'}}

        valid = harvester.amend_package(dataset)
        self.assertEqual(dataset['extras']['sector'], 'andere')
        self.assertFalse(valid)

    def test_type_amendment(self):

        harvester = BerlinCKANHarvester()

        package = {'type': None,
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None}}

        valid = harvester.amend_package(package)
        self.assertEqual(package['type'], 'datensatz')
        self.assertTrue(valid)

        package = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None}}

        valid = harvester.amend_package(package)
        self.assertEqual(package['type'], 'datensatz')
        self.assertTrue(valid)

        package = {'type': 'dokument',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None}}

        valid = harvester.amend_package(package)
        self.assertEqual(package['type'], 'dokument')
        self.assertTrue(valid)

        package = {'type': 'app',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None}}

        valid = harvester.amend_package(package)
        self.assertEqual(package['type'], 'app')
        self.assertTrue(valid)

        package = {'type': 'garbage',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None}}

        valid = harvester.amend_package(package)
        self.assertEqual(package['type'], 'datensatz')
        self.assertTrue(valid)

    def test_amend_portal(self):

        harvester = BerlinCKANHarvester()
        default = 'http://datenregister.berlin.de'

        dataset = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {}}

        valid = harvester.amend_package(dataset)
        portal = dataset['extras']['metadata_original_portal']
        self.assertEqual(portal, default)
        self.assertTrue(valid)

        dataset = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': None}}

        valid = harvester.amend_package(dataset)
        portal = dataset['extras']['metadata_original_portal']
        self.assertEqual(portal, default)
        self.assertTrue(valid)

        dataset = {'type': 'datensatz',
                   'groups': [],
                   'license_id': None,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'extras': {'metadata_original_portal': 'www.example.com'}}

        valid = harvester.amend_package(dataset)
        portal = dataset['extras']['metadata_original_portal']
        self.assertEqual(portal, 'www.example.com')
        self.assertTrue(valid)

    def test_amend_package(self):

        package = {'license_title': '',
                   'maintainer': '',
                   'maintainer_email': '',
                   'id': 'f998d542-c652-467e-b31b-c3e5d0300589',
                   'metadata_created': '2013-03-11T11:53:20.283753',
                   'relationships': [],
                   'license': None,
                   'metadata_modified': '2013-03-11T11:53:20.283753',
                   'author': '',
                   'author_email': '',
                   'state': 'active',
                   'version': '',
                   'license_id': '',
                   'type': None,
                   'resources': [],
                   'tags': [],
                   'tracking_summary': {'total': 0, 'recent': 0},
                   'groups': ['arbeit', 'geo', 'umwelt', 'wohnen'],
                   'name': 'test-dataset',
                   'isopen': False,
                   'notes_rendered': '',
                   'url': '',
                   'notes': '',
                   'title': 'Test Dataset',
                   'ratings_average': None,
                   'extras': {},
                   'ratings_count': 0,
                   'resources': [{'format':'CSV', 'url': 'http://travis-ci.org'}],
                   'revision_id': '411b25f9-1b8f-4f2a-90ae-05d3e8ff8d33'}

        harvester = BerlinCKANHarvester()

        self.assertEqual(package['license_id'], '')
        self.assertEqual(len(package['groups']), 4)
        self.assertTrue('arbeit' in package['groups'])
        self.assertTrue('geo' in package['groups'])
        self.assertTrue('umwelt' in package['groups'])
        self.assertTrue('wohnen' in package['groups'])

        harvester.amend_package(package)

        self.assertEqual(package['license_id'], 'notspecified')
        self.assertEqual(len(package['groups']), 4)
        self.assertTrue('wirtschaft_arbeit' in package['groups'])
        self.assertTrue('geo' in package['groups'])
        self.assertTrue('umwelt_klima' in package['groups'])
        self.assertTrue('infrastruktur_bauen_wohnen' in package['groups'])


class MoersHarvesterTest(unittest.TestCase):

    def test_title_amendment(self):

        publisher = {'role': 'veroeffentlichende_stelle',
                     'name': 'name',
                     'email': 'email'}

        maintainer = {'role': 'ansprechpartner',
                      'name': 'name',
                      'email': 'email'}

        valid_package = {'name': 'name',
                         'title': 'Adressen in Moers',
                         'resources': [],
                         'tags': [],
                         'extras': {'contacts': [publisher, maintainer],
                                    'terms_of_use': {'license_id': 'id'}}}

        invalid_package = {'name': 'name',
                          'title': 'Straßennamen',
                          'resources': [],
                          'tags': [],
                          'extras': {'contacts': [publisher, maintainer],
                                     'terms_of_use': {'license_id': 'id'}}}

        harvester = MoersCKANHarvester()
        harvester.amend_package(valid_package)
        harvester.amend_package(invalid_package)
        self.assertEqual(valid_package['title'], 'Adressen in Moers')
        self.assertEqual(invalid_package['title'], 'Straßennamen Moers')

    def test_amend_package(self):
        directory = os.path.dirname(os.path.abspath(__file__))
        moers_file = open(directory + '/moers.json')
        package = json.loads(moers_file.read())

        harvester = MoersCKANHarvester()
        harvester.amend_package(package)

        self.assertIsNotNone(package['id'])
        self.assertTrue(package['title'].endswith(' Moers'))

        self.assertNotIn(u'ü', package['name'])
        self.assertNotIn(u'ö', package['name'])
        self.assertNotIn(u'ä', package['name'])
        self.assertNotIn(u'ß', package['name'])

        self.assertEqual(package['author'], 'Stadt Moers')
        self.assertEqual(package['author_email'], 'Claus.Arndt@Moers.de')
        self.assertEqual(package['maintainer'], 'Ilona Bajorat')
        self.assertEqual(package['maintainer_email'], 'Ilona.Bajorat@Moers.de')

        self.assertEqual(package['license_id'], 'dl-de-by-1.0')
        self.assertEqual(package['extras']['metadata_original_portal'],
                         'http://www.offenedaten.moers.de/')

        self.assertEqual(package['extras']['spatial-text'],
                         '05 1 70 024 Moers')

        self.assertEqual(package['resources'][0]['format'], 'json')


class RLPHarvesterTest(unittest.TestCase):

    def test_gdi_rlp_package(self):

        package = {'author':                   'RLP',
                   'author_email':             'rlp@rlp.de',
                   'groups':                   ['gdi-rp', 'geo'],
                   'license_id':               'cc-by',
                   'point_of_contact':         None,
                   'point_of_contact_address': {'email': None},
                   'resources':                [{'format': 'pdf'}],
                   'type':                     None,
                   'extras':                   {'content_type': 'Kartenebene',
                                                'terms_of_use': {'license_id':
                                                                 'cc-by'}}}

        harvester = RLPCKANHarvester()
        harvester.amend_package(package)
        self.assertNotIn('gdi-rp', package['groups'])
        self.assertIn('geo', package['groups'])
        self.assertEqual(package['type'], 'datensatz')
