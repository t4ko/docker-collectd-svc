#!/usr/bin/env python
#
# vim: tabstop=4 shiftwidth=4

# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; only version 2 of the License is applicable.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
#
# Authors:
#   Ricardo Rocha <ricardo@catalyst.net.nz>
#
# About this plugin:
#   Helper object for all plugins.
#
# collectd:
#   http://collectd.org
# collectd-python:
#   http://collectd.org/documentation/manpages/collectd-python.5.shtml
#

import collectd
import signal
import datetime
import time
import traceback
import subprocess

class Base(object):

    def __init__(self):
        self.verbose = False
        self.debug = False
        self.prefix = ''
        self.cluster = ''
        self.name = 'client.admin'
        self.sshAdress = ''
        self.sshUser = ''
        self.sshRSAkey = ''
        self.interval = 60.0
        self.cluster_handle = None
        self.time = 0
        self.forcedTime = 0
        self.vdisksStatsCount = 0
        self.mdisksStatsCount = 0
        self.portsStatsCount = 0
        self.nodesStatsCount = 0

    def config_callback(self, conf):
        """Takes a collectd conf object and fills in the local config."""
        for node in conf.children:
            if node.key == "Verbose":
                if node.values[0] in ['True', 'true']:
                    self.verbose = True
            elif node.key == "Debug":
                if node.values[0] in ['True', 'true']:
                    self.debug = True
            elif node.key == "Prefix":
                self.prefix = node.values[0]
            elif node.key == 'Cluster':
                self.cluster = node.values[0]
            elif node.key == 'sshAdress':
                self.sshAdress = node.values[0]
            elif node.key == 'sshUser':
                self.sshUser = node.values[0]
            elif node.key == 'sshRSAkey':
                self.sshRSAkey = node.values[0]
            elif node.key == 'Interval':
                self.interval = float(node.values[0])
            else:
                collectd.warning("%s: unknown config key: %s" % (self.prefix, node.key))

    def dispatch(self, stats):
        """
        Dispatches the given stats.

        stats should be something like:

        {'plugin': {'plugin_instance': {'type': {'type_instance': <value>, ...}}}}
        """
        if not stats:
            collectd.error("%s: failed to retrieve stats" % self.prefix)
            return

        self.logdebug("dispatching %d new stats :: %s" % (len(stats), stats))
        self.logdebug("Timestamp passed to carbon database is {}".format(self.time))
        try:
            for plugin in stats.keys():
                for plugin_instance in stats[plugin].keys():
                    for type in stats[plugin][plugin_instance].keys():
                        if type == "tags":
                            continue
                        type_value = stats[plugin][plugin_instance][type]
                        if not isinstance(type_value, dict):
                            self.dispatch_value(plugin, plugin_instance, type, None, "", type_value)
                        else:
                          for type_instance in stats[plugin][plugin_instance][type].keys():
                              self.dispatch_value(plugin, plugin_instance,
                                      type, type_instance, stats[plugin][plugin_instance]['tags'],
                                      stats[plugin][plugin_instance][type][type_instance])
        except Exception as exc:
            collectd.error("%s: failed to dispatch values :: %s :: %s"
                    % (self.prefix, exc, traceback.format_exc()))

    def dispatch_value(self, plugin, plugin_instance, type, type_instance, tags, value):
        """Looks for the given stat in stats, and dispatches it"""
        self.logdebug("dispatching value %s.%s.%s.%s%s %s %s"
                % (plugin, plugin_instance, type, type_instance, tags, value, self.time))

        val = collectd.Values(type)
        val.plugin=plugin
        val.plugin_instance=plugin_instance
        if type_instance is not None:
            val.type_instance=type_instance
        else:
            val.type_instance=type
        val.type_instance=val.type_instance+tags #Add tags
        val.values=[value]
        val.interval = self.interval
        val.dispatch(time=self.time) #passed time is UTC 
        if ".vdisk" in plugin:
            self.vdisksStatsCount +=1
        elif ".mdiskgrp" in plugin:
            self.mdisksStatsCount += 1
        elif ".port" in plugin:
            self.portsStatsCount += 1
        elif ".node" in plugin:
            self.nodesStatsCount += 1

        self.logdebug("Sent metric dispatching value %s.%s.%s.%s%s %s %s"
                % (plugin, plugin_instance, type, type_instance, tags, value, self.time))

    def read_callback(self, timestamp = 0):
        self.forcedTime = timestamp
        try:
            start = time.time()
            stats = self.get_stats()
            self.dispatch(stats)
            if stats is not None:
                collectd.info("%s : Metrics collected : vdisks %d, mdiskgroups %d, ports %d, nodes %d : in %d sec"
                        % (self.cluster, self.vdisksStatsCount, self.mdisksStatsCount, self.portsStatsCount, self.nodesStatsCount, int(time.time() - start)))
            self.vdisksStatsCount = 0
            self.mdisksStatsCount = 0
            self.portsStatsCount = 0
            self.nodesStatsCount = 0
        except Exception as exc:
            collectd.error("%s: failed to get stats :: %s :: %s"
                    % (self.prefix, exc, traceback.format_exc()))

    def get_stats(self):
        collectd.error('Not implemented, should be subclassed')

    def logverbose(self, msg):
        if self.verbose:
            collectd.info("%s : %s : [VER] : %s" % (self.prefix, time.strftime("%H:%M:%S", time.localtime()), msg))

    def logdebug(self, msg):
        if self.debug:
            collectd.info("%s : %s : [DEB] : %s" % (self.prefix, time.strftime("%H:%M:%S", time.localtime()), msg))

    def loginfo(self, msg):
        collectd.info("%s : %s : [INF] : %s" % (self.prefix, time.strftime("%H:%M:%S", time.localtime()), msg))

    def logerror(self, msg):
        collectd.error("%s : %s : [ERR] : %s" % (self.prefix, time.strftime("%H:%M:%S", time.localtime()), msg))
        
    @staticmethod
    def reset_sigchld():
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
