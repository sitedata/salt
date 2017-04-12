# -*- coding: utf-8 -*-
'''
Module to do magic when calling execution modules on Minions

:maturity:      new
:platform:      Linux
'''

from __future__ import absolute_import

import logging
import random
import time

from salt.exceptions import CommandExecutionError

# pylint: disable=invalid-name
log = logging.getLogger(__name__)


def __virtual__():
    return 'magic'


def run_with_delay(function, *args, **kwargs):
    '''
    Returns the changes applied by a `jid`

    All arguments and keyword arguments are passed to the execution module

    function
        Salt function to call.

    min_delay:
        Set the lower possible value of the random delay. In seconds. (Default: 0)

    max_delay:
        Set the higher possible value of the random delay. In seconds (Default: 10)

    CLI Example:

    .. code-block:: bash

        salt '*' magic.run_with_delay cmd.run uptime
    '''
    if 'fixed_delay' in kwargs:
        _delay = kwargs['fixed_delay']
    elif 'minion_delay' in kwargs:
        _delay = kwargs['minion_delay'][__opts__['id']]
    else:
        _delay = random.randint(
            kwargs.get('min_delay', 0),
            kwargs.get('max_delay', 10)
        )

    func_kwargs = dict((k, v) for k, v in kwargs.items() if not k.startswith('__'))
    kwargs = dict((k, v) for k, v in kwargs.items() if k.startswith('__'))

    if function not in __salt__:
        raise CommandExecutionError(
            'function "{0}" does not exist'.format(function)
        )

    try:
        log.debug('Sleeping during {0} seconds before calling '
                  'execution module'.format(_delay))
        time.sleep(_delay)
        ret = __salt__[function](*args, **func_kwargs)
    except CommandExecutionError as exc:
        ret = "\n".join([str(exc), __salt__[function].__doc__])

    return ret
