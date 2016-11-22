#!/usr/bin/env python
""" Graphite module """
#
#

import socket
import time
import os

GRAPHITE_SERVER = '{{ graphite_host }}'
GRAPHITE_PORT = {{ graphite_port }}
PREFIX = 'ansible.'


class Graphite(object):
    """
    Graphite Client that will setup a TCP connection to graphite.
    :param graphite_server: hostname or ip address of graphite server
    :param graphite_port: TCP port we will connect to
    """

    def __init__(self, **kwargs):
        """
        Setup the connection to the graphite server
        """
        prefix = kwargs.get('prefix', None)
        system_name = kwargs.get('system_name', None)
        graphite_server = kwargs.get('graphite_server', None)
        graphite_port = kwargs.get('graphite_port', None)
        timeout_in_seconds = kwargs.get('timeout_in_seconds', 5)

        if not graphite_server:
            graphite_server = GRAPHITE_SERVER

        if not graphite_port:
            graphite_port = GRAPHITE_PORT

        self.addr = (graphite_server, graphite_port)
        self.timeout_in_seconds = int(timeout_in_seconds)

        tmp_prefix = self.__set_prefix(prefix)
        tmp_sname = self.__set_sname(system_name)

        self.prefix = self.__set_valprefix(tmp_prefix, tmp_sname)

    @staticmethod
    def __set_prefix(prefix):
        if prefix is None:
            return PREFIX
        elif prefix == '':
            return ''
        else:
            return "%s." % prefix

    @staticmethod
    def __set_sname(system_name):
        if system_name is None:
            return '%s.' % os.uname()[1].replace('.', '_')
        elif system_name == '':
            return ''
        else:
            return '%s.' % system_name

    @staticmethod
    def __set_valprefix(tmp_prefix, tmp_sname):
        prefix = "%s%s" % (tmp_prefix, tmp_sname)
        if '..' in prefix:
            prefix = prefix.replace('..', '.')
        if ' ' in prefix:
            prefix = prefix.replace(' ', '_')
        if '/' in prefix:
            prefix = prefix.replace('/', 'r')
        return prefix

    def __connect(self):
        """
        Make a TCP connection to the graphite server on port self.port
        """

        mysocket = socket.socket()
        mysocket.settimeout(self.timeout_in_seconds)
        try:
            mysocket.connect(self.addr)
        except socket.timeout:
            raise GraphiteSendException(
                "Took over %d second(s) to connect to %s" %
                (self.timeout_in_seconds, self.addr))
        except socket.gaierror:
            raise GraphiteSendException(
                "No address associated with hostname %s:%s" % self.addr)
        except Exception as error:
            raise GraphiteSendException(
                "unknown exception while connecting to %s - %s" %
                (self.addr, error)
            )
        return mysocket

    @staticmethod
    def __disconnect(mysocket):
        """
        Close the TCP connection with the graphite server.
        """

        try:
            mysocket.shutdown(1)

        except AttributeError:
            mysocket = None
        except Exception:
            mysocket = None

        finally:
            mysocket = None

    def send(self, metric, value, timestamp=None):
        """ Given a message send it to the graphite server. """

        if timestamp is None:
            timestamp = int(time.time())
        else:
            timestamp = int(timestamp)

        if type(value).__name__ in ['str', 'unicode']:
            value = float(value)

        if '/' in metric:
            metric = metric.replace('/', '-')

        message = "%s %f %d\n" % (self.prefix+metric, value, timestamp)

        mysocket = self.__connect()

        try:
            mysocket.sendall(message)

        except socket.gaierror as error:
            self.__disconnect(mysocket)
            raise GraphiteSendException(
                "Failed to send data to %s, with error: %s" %
                (self.addr, error))
        return message.strip()


class GraphiteSendException(Exception):
    """ Some doc here """
    pass
