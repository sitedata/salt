# encoding: utf-8
'''
Make api awesomeness
'''
from __future__ import absolute_import
# Import Python libs
import inspect
from multiprocessing import Lock, Process, Value
import os
import time
import random

# Import Salt libs
import salt.log  # pylint: disable=W0611
import salt.client
import salt.config
import salt.runner
import salt.syspaths
import salt.wheel
import salt.utils
import salt.client.ssh.client
import salt.exceptions


import logging
log = logging.getLogger(__name__)


class TokenBucket(object):
    '''
    Token Bucket rate limiting mechanism. To enable it the following settings
    to your /etc/salt/master configuration file.

    token_bucket_rate_limiting_size: 10
    '''
    def __init__(self, opts):
        if 'token_bucket_rate_limiting_size' in opts:
            self.bucket_size = opts['token_bucket_rate_limiting_size']
            self.tokens = Value('i', self.bucket_size)
            self.lock = Lock()
            self.ready = True
        else:
            self.ready = False

    def __feed_bucket(self):
        if self.tokens.value < self.bucket_size:
            with self.lock:
                self.tokens.value += 1
                log.debug("--> FEEDING BUCKET: bucket status: {} tokens".format(self.tokens.value))

    def loop(self):
        log.info('Token bucket Rate Limiting is enabled. '
                 'Bucket size: {0}'.format(self.bucket_size))
        while True:
            self.__feed_bucket()
            time.sleep(1)

    def get_tokens(self):
        with self.lock:
            return self.tokens.value

    def consume_tokens(self, num):
        with self.lock:
            self.tokens.value -= num
            log.debug("--> CONSUME {0} TOKENs: bucket status: {1} tokens".format(num, self.tokens.value))

    def is_ready(self):
        return self.ready

    def convert_to_delayed_job(self, _kwargs):
        if _kwargs['fun']:
            if 'arg' not in _kwargs:
                _kwargs['arg'] = _kwargs['fun']
            else:
                _kwargs['arg'].insert(0, _kwargs['fun'])
            _kwargs['fun'] = 'magic.run_with_delay'
            # If tgt is a glob we use a fixed delay for all minions
            if not isinstance(_kwargs['tgt'], list):
                _kwargs['kwarg']['fixed_delay'] = random.randint(1, 5)
            else:
                _kwargs['kwarg']['minion_delay'] = {}
                for tgt in _kwargs['tgt']:
                    _kwargs['kwarg']['minion_delay'][tgt] = random.randint(1, 5)


class NetapiClient(object):
    '''
    Provide a uniform method of accessing the various client interfaces in Salt
    in the form of low-data data structures. For example:

    >>> client = NetapiClient(__opts__)
    >>> lowstate = {'client': 'local', 'tgt': '*', 'fun': 'test.ping', 'arg': ''}
    >>> client.run(lowstate)
    '''

    def __init__(self, opts, token_bucket_start=False):
        self.opts = opts
        if token_bucket_start:
           self.token_bucket = TokenBucket(self.opts)
           if self.token_bucket.is_ready():
               self._bucket_proc = Process(target=self.token_bucket.loop, args=())
               self._bucket_proc.start()

    def _is_master_running(self):
        '''
        Perform a lightweight check to see if the master daemon is running

        Note, this will return an invalid success if the master crashed or was
        not shut down cleanly.
        '''
        if self.opts['transport'] == 'tcp':
            ipc_file = 'publish_pull.ipc'
        else:
            ipc_file = 'workers.ipc'
        return os.path.exists(os.path.join(
            self.opts['sock_dir'],
            ipc_file))

    def run(self, low):
        '''
        Execute the specified function in the specified client by passing the
        lowstate
        '''
        # Eauth currently requires a running daemon and commands run through
        # this method require eauth so perform a quick check to raise a
        # more meaningful error.
        if not self._is_master_running():
            raise salt.exceptions.SaltDaemonNotRunning(
                    'Salt Master is not available.')

        if low.get('client') not in CLIENTS:
            raise salt.exceptions.SaltInvocationError('Invalid client specified')

        if not ('token' in low or 'eauth' in low) and low['client'] != 'ssh':
            raise salt.exceptions.EauthAuthenticationError(
                    'No authentication credentials given')

        l_fun = getattr(self, low['client'])
        f_call = salt.utils.format_call(l_fun, low)
        return l_fun(*f_call.get('args', ()), **f_call.get('kwargs', {}))

    def local_token_bucket_async(self, *args, **kwargs):
        '''
        Run :ref:`execution modules <all-salt.modules>` asynchronously
        using the token bucket algorithm.

        Wraps :py:meth:`salt.client.LocalClient.run_job`.

        :return: job ID
        '''
        if not self.token_bucket.is_ready():
            _msg = 'Client not available. Token bucket is not configured'
            log.error(_msg)
            raise salt.exceptions.SaltInvocationError(_msg)

        local = salt.client.get_local_client(mopts=self.opts)
        if self.token_bucket.get_tokens():
            self.token_bucket.consume_tokens(1)
        else:
            self.token_bucket.convert_to_delayed_job(kwargs)
        log.debug("Publishing final payload: {0}".format(kwargs))
        return local.run_job(*args, **kwargs)

    def local_async(self, *args, **kwargs):
        '''
        Run :ref:`execution modules <all-salt.modules>` asynchronously

        Wraps :py:meth:`salt.client.LocalClient.run_job`.

        :return: job ID
        '''
        local = salt.client.get_local_client(mopts=self.opts)
        return local.run_job(*args, **kwargs)

    def local(self, *args, **kwargs):
        '''
        Run :ref:`execution modules <all-salt.modules>` synchronously

        See :py:meth:`salt.client.LocalClient.cmd` for all available
        parameters.

        Sends a command from the master to the targeted minions. This is the
        same interface that Salt's own CLI uses. Note the ``arg`` and ``kwarg``
        parameters are sent down to the minion(s) and the given function,
        ``fun``, is called with those parameters.

        :return: Returns the result from the execution module
        '''
        local = salt.client.get_local_client(mopts=self.opts)
        return local.cmd(*args, **kwargs)

    def local_subset(self, *args, **kwargs):
        '''
        Run :ref:`execution modules <all-salt.modules>` against subsets of minions

        .. versionadded:: 2016.3.0

        Wraps :py:meth:`salt.client.LocalClient.cmd_subset`
        '''
        local = salt.client.get_local_client(mopts=self.opts)
        return local.cmd_subset(*args, **kwargs)

    def ssh(self, *args, **kwargs):
        '''
        Run salt-ssh commands synchronously

        Wraps :py:meth:`salt.client.ssh.client.SSHClient.cmd_sync`.

        :return: Returns the result from the salt-ssh command
        '''
        ssh_client = salt.client.ssh.client.SSHClient(mopts=self.opts,
                                                      disable_custom_roster=True)
        return ssh_client.cmd_sync(kwargs)

    def runner(self, fun, timeout=None, **kwargs):
        '''
        Run `runner modules <all-salt.runners>` synchronously

        Wraps :py:meth:`salt.runner.RunnerClient.cmd_sync`.

        Note that runner functions must be called using keyword arguments.
        Positional arguments are not supported.

        :return: Returns the result from the runner module
        '''
        kwargs['fun'] = fun
        runner = salt.runner.RunnerClient(self.opts)
        return runner.cmd_sync(kwargs, timeout=timeout)

    def runner_async(self, fun, **kwargs):
        '''
        Run `runner modules <all-salt.runners>` asynchronously

        Wraps :py:meth:`salt.runner.RunnerClient.cmd_async`.

        Note that runner functions must be called using keyword arguments.
        Positional arguments are not supported.

        :return: event data and a job ID for the executed function.
        '''
        kwargs['fun'] = fun
        runner = salt.runner.RunnerClient(self.opts)
        return runner.cmd_async(kwargs)

    def wheel(self, fun, **kwargs):
        '''
        Run :ref:`wheel modules <all-salt.wheel>` synchronously

        Wraps :py:meth:`salt.wheel.WheelClient.master_call`.

        Note that wheel functions must be called using keyword arguments.
        Positional arguments are not supported.

        :return: Returns the result from the wheel module
        '''
        kwargs['fun'] = fun
        wheel = salt.wheel.WheelClient(self.opts)
        return wheel.cmd_sync(kwargs)

    def wheel_async(self, fun, **kwargs):
        '''
        Run :ref:`wheel modules <all-salt.wheel>` asynchronously

        Wraps :py:meth:`salt.wheel.WheelClient.master_call`.

        Note that wheel functions must be called using keyword arguments.
        Positional arguments are not supported.

        :return: Returns the result from the wheel module
        '''
        kwargs['fun'] = fun
        wheel = salt.wheel.WheelClient(self.opts)
        return wheel.cmd_async(kwargs)

CLIENTS = [
    name for name, _
    in inspect.getmembers(NetapiClient, predicate=inspect.ismethod)
    if not (name == 'run' or name.startswith('_'))
]
