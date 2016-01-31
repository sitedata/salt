# -*- coding: utf-8 -*-
'''
Send events covering supervisord processes status
'''

# Import Python Libs
from __future__ import absolute_import

import logging

log = logging.getLogger(__name__)  # pylint: disable=invalid-name

LAST_STATUS = {}


def validate(config):
    '''
    Validate the beacon configuration
    '''
    # Configuration for supervisord beacon should be a dict
    if not isinstance(config, dict):
        log.info('Configuration for supervisord beacon must be a dictionary.')
        return False

    # Each program must be defined as dict
    for program in config:
        if not isinstance(config[program], dict):
            log.info('program configuration for supervisord beacon must be a dict.')
            return False

    return True


def beacon(config):
    '''
    Scan for the configured supervisor processes and fire events

    Example Config

    .. code-block:: yaml

        beacons:
          supervisord:
            myapp1:
              emitatstartup: True
            myapp2:
              onchangeonly: True
              user:
              conf_file:
              bin_env:

    The config above sets up beacons to check for
    the myapp1 and myapp2 supervisord processes.

    `emitatstartup`: when `emitatstartup` is False the beacon will not fire
    event when the minion is reload. Applicable only when `onchangeonly` is True.
    The default is True.

    `onchangeonly`: when `onchangeonly` is True the beacon will fire
    events only when the process status changes.  Otherwise, it will fire an
    event at each beacon interval.  The default is False.

    'user', 'conf_file' and 'bin_env' are optionals and
    specify the supervisord enviroment.
    '''
    ret = []

    if 'supervisord.status' not in __salt__:
        ret['result'] = False
        ret['comment'] = 'Supervisord module not activated. Do you need to ' \
                         'install supervisord?'
        return ret

    for process in config:
        # default parameters
        _defaults = {
            'emitatstartup': True,
            'onchangeonly': False,
        }

        # If no parameters are provided, defaults are assigned
        if config[process] is None:
            config[process] = _defaults

        ret_dict = __salt__['supervisord.status'](
            process,
            user=config[process].get('user'),
            conf_file=config[process].get('conf_file'),
            bin_env=config[process].get('bin_env'),
        )

        if not process in ret_dict:
           log.info('supervisord program "{}" is not defined in ' \
                    'configuration'.format(process))
           continue

        if config[process].get('onchangeonly', _defaults['onchangeonly']):
            if process not in LAST_STATUS:
                LAST_STATUS[process] = ret_dict[process]['state']
                if config[process].get('emitatstartup', _defaults['emitatstartup']):
                    ret_dict[process]['emitatstartup'] = True
                    ret.append(ret_dict)
                continue

            if LAST_STATUS[process] != ret_dict[process]['state']:
                LAST_STATUS[process] = ret_dict[process]['state']
                ret.append(ret_dict)

        else:
            ret.append(ret_dict)

    return ret
