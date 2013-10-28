#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ckan.lib.cli import CkanCommand


class Validator(CkanCommand):
    '''Validates datasets against the GovData.de JSON schema'''

    def __init__(self, name):
        super(Validator, self).__init__(name)

    def command(self):
        pass
