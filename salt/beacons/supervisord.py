# -*- coding: utf-8 -*-
'''
Send events covering supervisord status
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
              emitatstartup: False
            myapp2:
              emitatstartup: True
              onstatuschange: False
              user:
              conf_file:
              bin_env:


    The config above sets up beacons to check for
    the myapp1 and myapp2 supervisord processes.
    user, conf_file and bin_env are optionals and
    specify the supervisord enviroment.
    '''
    ret = []

    if 'supervisord.status' not in __salt__:
        ret['result'] = False
        ret['comment'] = 'Supervisord module not activated. Do you need to ' \
                         'install supervisord?'
        return ret

    for process in config:
        process_status = __salt__['supervisord.status'](
            process,
            user=config[process].get('user'),
            conf_file=config[process].get('conf_file'),
            bin_env=config[process].get('bin_env'),
        )

        if not process in process_status:
           log.info('supervisordprogram "{}" is not defined in ' \
                    'configuration'.format(process))
           continue

        ret_dict = {}
        ret_dict[process] = {'state': process_status[process]['state']}

        # default parameters
        _defaults = {
            'emitatstartup': False,
            'onstatuschange': True,
        }

        # If no parameters are provided, defaults are assigned
        if config[process] is None:
            config[process] = _defaults

        # Including PID when supervisord program state is RUNNING
        if ret_dict[process]['state'] == "RUNNING":
            pid, status = process_status[process]['reason'].split(",")
            ret_dict[process]['pid'] = pid

        # Emit the 'emitatstartup' event only if it's the first time
        # for this process_uid
        if process not in LAST_STATUS:
            LAST_STATUS[process] = ret_dict[process].copy()
            if config[process].get('emitatstartup', _defaults['emitatstartup']):
                ret_dict[process]['emitatstartup'] = True
                ret.append(ret_dict)
                continue

        if LAST_STATUS[process] != ret_dict[process]:
            LAST_STATUS[process] = ret_dict[process].copy()
            if config[process].get('onstatuschange', _defaults['onstatuschange']):
                ret_dict[process]['onstatuschange'] = True
                ret.append(ret_dict)

    return ret
