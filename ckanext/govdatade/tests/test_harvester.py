from ckanext.govdatade.harvesters.ckanharvester import BerlinCKANHarvester
from ckanext.govdatade.harvesters.ckanharvester import MoersCKANHarvester

import os
import json
import unittest


class BerlinHarvesterTest(unittest.TestCase):

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

    def test_amend_package(self):
        directory = os.path.dirname(os.path.abspath(__file__))
        moers_file = open(directory + '/moers.json')
        package = json.loads(moers_file.read())

        harvester = MoersCKANHarvester()
        harvester.amend_package(package)
