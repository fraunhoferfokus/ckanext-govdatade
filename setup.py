from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(
	name='ckanext-govdatade',
	version=version,
	description="GovData.de specific CKAN extension",
	long_description="""\
	""",
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='',
	author='Fraunhofer FOKUS',
	author_email='ogdd-harvesting@fokus.fraunhofer.de',
	url='https://github.com/fraunhoferfokus/ckanext-govdatade',
	license='AGPL',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext', 'ckanext.govdatade'],
	include_package_data=True,
	zip_safe=False,
	install_requires=[
		# -*- Extra requirements: -*-
	],
    entry_points=\
    """
    [ckan.plugins]
    bayern_harvester=ckanext.govdatade.harvesters.ckanharvester:BayernCKANHarvester
    bremen_harvester=ckanext.govdatade.harvesters.ckanharvester:BremenCKANHarvester
    hamburg_harvester=ckanext.govdatade.harvesters.ckanharvester:HamburgCKANHarvester
    rlp_harvester=ckanext.govdatade.harvesters.ckanharvester:RLPCKANHarvester
    berlin_harvester=ckanext.govdatade.harvesters.ckanharvester:BerlinCKANHarvester
    moers_harvester=ckanext.govdatade.harvesters.ckanharvester:MoersCKANHarvester
    rostock_harvester=ckanext.govdatade.harvesters.ckanharvester:RostockCKANHarvester
    govapps_harvester=ckanext.govdatade.harvesters.ckanharvester:GovAppsHarvester
    datahub_harvester=ckanext.govdatade.harvesters.ckanharvester:DatahubCKANHarvester

    destatis_harvester=ckanext.govdatade.harvesters.ckanharvester:DestatisZipHarvester
    koeln_harvester=ckanext.govdatade.harvesters.ckanharvester:KoelnCKANHarvester
   
    bkg_harvester=ckanext.govdatade.harvesters.ckanharvester:BKGHarvester
   
    [paste.paster_command]
    schemachecker = ckanext.govdatade.commands.schema_checker:SchemaChecker
    linkchecker = ckanext.govdatade.commands.link_checker:LinkChecker
    report = ckanext.govdatade.commands.report:Report

    [nose.plugins]
    pylons = pylons.test:PylonsPlugin
    """,
)
