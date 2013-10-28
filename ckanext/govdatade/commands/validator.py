#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ckan import model
from ckan.lib.cli import CkanCommand
from ckan.logic import get_action


class Validator(CkanCommand):
    '''Validates datasets against the GovData.de JSON schema'''

    summary = __doc__.split('\n')[0]

    def __init__(self, name):
        super(Validator, self).__init__(name)

    def command(self):
        context = {'model':       model,
                   'session':     model.Session,
                   'ignore_auth': True}
