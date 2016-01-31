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

    # programs dict is mandatory
    if not config.get('programs'):
        log.info('Configuration for supervisord must contains programs.')
        return False

    # Each program must be defined as dict
    for program in config['programs']:
        if not isinstance(config['programs'][program], dict):
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
            programs:
              myapp1:
		        emitatstartup: False
              myapp2:
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

    all_processes = __salt__['supervisord.status'](
        user=config.get('user'),
        conf_file=config.get('conf_file'),
        bin_env=config.get('bin_env'),
    )

    for process in config['programs']:
        if not all_processes.get(process):
           log.info('supervisord program "{}" is not defined in configuration'.format(process))
           continue

        ret_dict = {}
        ret_dict[process] = {'state': all_processes[process]['state']}

        # default parameters
        _defaults = {
            'emitatstartup': False,
            'onstatuschange': True,
        }

        # If no parameters are provided, defaults are assigned
        if config['programs'][process] is None:
            config['programs'][process] = _defaults

        # Including PID when supervisord program state is RUNNING
        if ret_dict[process]['state'] == "RUNNING":
            pid, status = all_processes[process]['reason'].split(",")
            ret_dict[process]['pid'] = pid

        # We use process_uid as key in LAST_STATUS dictionary.
        # Programs can coexist in different enviroments with identical name
        process_uid = [
            process,
            config.get("user"),
            config.get("conf_file"),
            config.get("bin_env")
        ]
        process_uid = "-".join(str(x) for x in process_uid)

        # Emit the 'emitatstartup' event only if it's the first time
        # for this process_uid
        if process_uid not in LAST_STATUS:
            LAST_STATUS[process_uid] = ret_dict[process].copy()
            if config['programs'][process].get('emitatstartup', _defaults['emitatstartup']):
                ret_dict[process]['emitatstartup'] = True
                ret.append(ret_dict)
                continue

        if LAST_STATUS[process_uid] != ret_dict[process]:
            LAST_STATUS[process_uid] = ret_dict[process].copy()
            if config['programs'][process].get('onstatuschange', _defaults['onstatuschange']):
                ret_dict[process]['onstatuschange'] = True
                ret.append(ret_dict)

    return ret
