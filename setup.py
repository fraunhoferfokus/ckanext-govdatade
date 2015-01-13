from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(
	name='ckanext-govdatade',
	version=version,
	description="GovData.de specific CKAN extension",
	long_description="""\
	""",
	classifiers=[],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
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
    hamburg_harvester=ckanext.govdatade.harvesters.ckanharvester:HamburgCKANHarvester
    rlp_harvester=ckanext.govdatade.harvesters.ckanharvester:RLPCKANHarvester
    berlin_harvester=ckanext.govdatade.harvesters.ckanharvester:BerlinCKANHarvester
    datahub_harvester=ckanext.govdatade.harvesters.ckanharvester:DatahubCKANHarvester
    koeln_harvester=ckanext.govdatade.harvesters.ckanharvester:KoelnCKANHarvester
    rostock_harvester=ckanext.govdatade.harvesters.ckanharvester:RostockCKANHarvester
    bayern_harvester=ckanext.govdatade.harvesters.jsonharvester:BayernCKANHarvester
    bremen_harvester=ckanext.govdatade.harvesters.jsonharvester:BremenCKANHarvester
    moers_harvester=ckanext.govdatade.harvesters.jsonharvester:MoersCKANHarvester
    govapps_harvester=ckanext.govdatade.harvesters.jsonharvester:GovAppsHarvester
    bkg_harvester=ckanext.govdatade.harvesters.jsonharvester:BKGHarvester
    destatis_harvester=ckanext.govdatade.harvesters.jsonharvester:DestatisZipHarvester
    destatis2_harvester=ckanext.govdatade.harvesters.jsonharvester:SecondDestatisZipHarvester
    regionalstatistik_harvester=ckanext.govdatade.harvesters.jsonharvester:RegionalStatistikZipHarvester

    [paste.paster_command]
    schemachecker = ckanext.govdatade.commands.schema_checker:SchemaChecker
    linkchecker = ckanext.govdatade.commands.link_checker:LinkChecker
    report = ckanext.govdatade.commands.report:Report

    [nose.plugins]
    pylons = pylons.test:PylonsPlugin
    """,
)
