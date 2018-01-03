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
#   Lucas Tronel <lucas.tronel@gmail.com>
#
# About this plugin:
#   This plugin collects information regarding IBM SVC Clusters.
#
# collectd:
#   http://collectd.org
# collectd-python:
#   http://collectd.org/documentation/manpages/collectd-python.5.shtml
# IBM SVC Clusters:
#   https://www.ibm.com/systems/storage/software/virtualization/svc/
#

import collectd
import json
import re
import traceback
import random
import paramiko
from scp import SCPClient
import os
import time
import pprint
pp = pprint.PrettyPrinter(indent=4, depth=None)
import xml.etree.cElementTree as ET
from collections import defaultdict

import base

class SVCPlugin(base.Base):

    def __init__(self):
        base.Base.__init__(self)
        self.prefix = 'svc'
        self.ssh = None
        self.stats_history = None
        self.dumps = {}
        self.catchup = {}
        self.timezone = None

    def allowWildcards(self, s):
        """Return a shell-escaped version of the string `s`."""
        _check_safe = re.compile(br'/dumps/iostats/\*[0-9]{6}_[0-9]{6}').search
        if not s:
            return b""
        if _check_safe(s) is None:
            self.loginfo("File name is not a dump file name")
            return b""
        return s

    def check_command(self, command, attempt=3):
        """Retry to send a command if the svc cluster can't answer, return false if all attempt failed"""
        commandSuccess = False
        originalAttempt = attempt
        while not commandSuccess and attempt > 0:
            (stdin, stdout, stderr) = self.ssh.exec_command(command)
            commandSuccess = True
            for errLine in list(stderr):
                self.logerror("STDERR : {}".format(errLine.replace('\n', '')))
                if "CMMVC" in errLine: # SVC CLI error
                    commandSuccess = False
            if commandSuccess:
                break
            attempt = attempt - 1 
            time.sleep(1)
        if attempt <= 0 and not commandSuccess:
            self.loginfo("Command {} failed {} times".format(command, originalAttempt))
            self.logverbose("Closing ssh connection")
            self.ssh.close()
        if attempt < originalAttempt and attempt > 0:
            self.logverbose("Command {} succeeded after {} retry".format(command, originalAttempt - attempt))
        return commandSuccess, stdout, stderr

    def check_ssh(self):
        """Check that the ssh connection is established properly, return a boolean"""
        if self.ssh is not None:
            transport = self.ssh.get_transport()
        if self.ssh is None or (self.ssh is not None and (not transport or (transport and not transport.is_active()))):
            if self.ssh is not None and transport and not transport.is_active():
                self.ssh.close()
                self.logverbose("SSH connection not properly established, restarting connection")
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
            self.ssh.connect(self.sshAdress, username=self.sshUser, key_filename=self.sshRSAkey, compress=True)
            self.logverbose("Successfuly connected with ssh")
            return False
        else:
            return True

    def get_stats(self):
        """Retrieves stats from the svc cluster pools"""

        svc_cluster = self.cluster # Defines the name of the current svc cluster (provided in the conf)
        clusternode = "{}.node".format(self.cluster)
        clustermdsk = "{}.mdsk".format(self.cluster)
        clustervdsk = "{}.vdsk".format(self.cluster)
        vdisks, mdisks, nodes = 'vdisks', 'mdisks', 'nodes'

        # Close previous ssh connections if still alive
        if self.ssh is not None and self.forcedTime == 0:
            transport = self.ssh.get_transport()
            if transport and transport.is_active():
                self.logverbose("Closing existing ssh connection")
                self.ssh.close()

        self.logverbose("Beginning stats collection")

        # Connect with ssh to svc
        if self.check_ssh():
            self.logverbose("SSH connection is still alive, not opening a new one")

        # Load the node list
        self.logverbose("Loading the node list")

        (success, stdout, stderr) = self.check_command('lsnode -delim :')

        nodes_hash = {}
        config_node =''
        nodeEncIdList = []
        stdout = list(stdout)
        headers = stdout.pop(0)[:-1].split(':')

        for line in stdout:
            fields = line[:-1].split(':')
            nodes_hash[fields[1]] = {}
            i=0
            for hd in headers:
                nodes_hash[fields[1]][hd] = fields[i]
                i += 1

        self.logdebug("%s" % list(nodes_hash.keys()))

        # Get stats files from each node
        self.logverbose("Getting stats files for each nodes")

        for node in nodes_hash.keys():

            (success, stdout, stderr) = self.check_command('lsdumps -prefix /dumps/iostats/ -nohdr -delim : %s' % node)

            nodes_hash[node]['files'] = []

            nodeEncIdList.append(nodes_hash[node]['enclosure_id'])

            for line in reversed(list(stdout)):
                nodes_hash[node]['files'].append(line[:-1].split(':')[1])

            if nodes_hash[node]['config_node'] == 'yes':
                config_node = node
            else:
                nodes_hash[node]['files'] = nodes_hash[node]['files'][:16]

            self.logdebug("%s(%s): found %s file(s)" % (node, nodes_hash[node]['enclosure_id'], len(nodes_hash[node]['files'])))

        # Copy missing stats files on config node
        self.logverbose("Copying last missing stats files on config node")

        for node in nodes_hash.keys():

            if nodes_hash[node]['config_node'] == 'yes' : continue

            missing_file = set(nodes_hash[node]['files']) - set(nodes_hash[config_node]['files'])

            pattern = re.compile(".*_%s_.*" % nodes_hash[node]['enclosure_id'])

            for file in missing_file:
                if pattern.match(file):
                    self.logdebug("copying %s from %s" % (file, node))
                    (success, stdout, stderr) = self.check_command('cpdumps -prefix /dumps/iostats/%s %s' % (file, node))

        # Load the timezone
        if self.timezone == None:
            (success, stdout, stderr) = self.check_command('showtimezone -nohdr -delim :')
            if not success: return
            for line in stdout:
                self.timezone = line.split(':')[1].replace('\n', '')
                break
            os.environ['TZ'] = self.timezone
            time.tzset()
            self.logverbose("Working timezone set to {} {}".format(os.environ['TZ'], time.strftime("%z", time.localtime())))

        #Get the time at which all nodes made their iostats dump 
        self.logverbose("Searching the time at which all dumps are available")
        (success, stdout, stderr) = self.check_command('lsdumps -prefix /dumps/iostats/ -nohdr')
        if not success: return
        timestamps = {}
        lsdumpsList = set()
        dumpCount = len(nodeEncIdList) * 4
        self.logdebug("Lsdumps returns : ")
        for line in reversed(list(stdout)):
            line = line.replace('\n', '')
            line = line.split(' N')[1]
            line = 'N{}'.format(line)
            self.logdebug(line)
            statsType, junk, node, day, minute = line.split('_')
            timeString = "{0}_{1}".format(day, minute[:6])
            lsdumpsList.add("{}_stats_{}_{}".format(statsType, node, timeString))
            epoch = time.mktime(time.strptime(timeString[:-2], "%y%m%d_%H%M"))
            if epoch in timestamps:
                timestamps[epoch]['counter'] = timestamps[epoch]['counter'] + 1
            else:
                timestamps[epoch] = {
                    'string' : timeString,
                    'counter' : 1
                }
        self.logdebug("lsdumps set contains :\n %s" % pprint.pformat(lsdumpsList))
        self.logdebug("timestamps available :\n %s" % pprint.pformat(timestamps))

        currentTime = 0

        if self.forcedTime == 0: # Don't update the timestamp if the time is forced
            currentTime = self.time
            for epoch in sorted(timestamps.keys(), reverse=True):
                if timestamps[epoch]['counter'] == dumpCount : # All file are available for this timestamp 
                    self.logverbose("Most recent dumps available use timestamp {}".format(timestamps[epoch]['string']))
                    if self.time != 0 and epoch > self.time + self.interval: # If the most recent timestamps is more than the interval
                        while self.time != epoch - self.interval: # Add missing timestamps to the catchup list
                            self.time = self.time + self.interval
                            temptimestring = time.strftime("%y%m%d_%H%M", time.localtime(self.time))
                            self.logverbose("Intermediate stats with timestamp {} will be collected later".format(temptimestring))
                            self.catchup[self.time] = temptimestring  
                    elif self.time != 0 and epoch < self.time + self.interval:  # If the most recent timestamp is less than the interval
                        break
                    if epoch == self.time + self.interval or self.time == 0: # If timestamp match interval or there is no previous collect
                        currentTime = epoch
                    break

            #Catch up available dumps collect
            for catchupEpoch in sorted(self.catchup.keys()):
                if (catchupEpoch in timestamps) and (timestamps[catchupEpoch]['counter'] == dumpCount): # The dumps are still on the cluster
                    self.logverbose("Catching up stats collection for timestamp {}".format(self.catchup[catchupEpoch]))
                    del self.catchup[catchupEpoch]
                    self.read_callback(timestamp=(catchupEpoch))
                else:
                    tempCount = 0
                    for epoch in sorted(timestamps.keys()):
                        if int(epoch) >= int(catchupEpoch):
                            tempCount = tempCount + 1
                            if tempCount >= 15: #Remove outdated timestamps that can't be collected
                                self.logverbose("Stats dumps are no more available for timestamps {}".format(self.catchup[catchupEpoch]))
                                del self.catchup[catchupEpoch]
                                break
            self.forcedTime = 0
            self.time = currentTime
        else:
            self.time = self.forcedTime

        # Compute old timestamp
        if self.time in timestamps:
            newTimeString = timestamps[self.time]['string']
        else:
            self.logverbose("Closing ssh connection")
            self.ssh.close()
            return
        self.logverbose("Collecting stats for timestamp {}".format(newTimeString))
        oldEpoch = self.time - self.interval
        if oldEpoch in timestamps:
            oldTimeString = timestamps[oldEpoch]['string']
        else:
            oldTimeString = 'XXXXXX_XXXXXX'

        # Create the dumps directory if it does not exist yet
        dumpsFolder = '{}/svc-stats-dumps'.format(os.getcwd())
        if not os.path.exists(dumpsFolder):
            os.makedirs(dumpsFolder)

        # Check if the last available dumps have not already been collected
        dumpsList = os.listdir(dumpsFolder)
        if not self.forcedTime:
            for dumpName in dumpsList:
                if newTimeString in dumpName:
                    self.loginfo("New stats dumps are not yet available")
                    self.logverbose("Closing ssh connection")
                    self.ssh.close()
                    return

        # Check if files from previous stats are already in the directory
        oldFileDownloaded, oldFileAvailable = True, True
        oldDumpsList = set()
        self.logdebug("Before doing anything the dumps folder contains :\n {}".format(str(dumpsList)))
        self.logdebug("Old dumps list contains :")
        for nodeId in nodeEncIdList:
            for statType in ['Nn', 'Nv', 'Nm']:
                oldFileName = "{0}_stats_{1}_{2}".format(statType, nodeId, oldTimeString)
                oldDumpsList.add(oldFileName)
                self.logdebug(oldFileName)
                if oldFileName not in dumpsList:
                    oldFileDownloaded = False

        # Download the file from the SVC cluster if they are available
        self.check_ssh()
        self.logverbose("Downloading the dumps with scp")
        t = self.ssh.get_transport()
        scp = SCPClient(t, socket_timeout=30.0, sanitize=self.allowWildcards)

        if not oldFileDownloaded:
            for oldFileName in oldDumpsList:
                if oldFileName not in lsdumpsList:
                    self.loginfo("Stats dumps from previous interval are not available")
                    return
        if not oldFileDownloaded and oldFileAvailable:
            self.logverbose("Downloading old and new dumps")
            self.logdebug("String passed to scp.get is : /dumps/iostats/*{} /dumps/iostats/*{}".format(oldTimeString, newTimeString))
            try:
                scp.get("/dumps/iostats/*{} /dumps/iostats/*{}".format(oldTimeString, newTimeString), dumpsFolder)
            except:
                self.logerror("SCP error while downloading dumps, retrying")
                self.catchup[self.time] = newTimeString
                return
        elif oldFileDownloaded or not oldFileAvailable:
            self.logverbose("Downloading new dumps")
            self.logdebug("String passed to scp.get is : /dumps/iostats/*{}".format(newTimeString))
            try:
                scp.get("/dumps/iostats/*{}".format(newTimeString), dumpsFolder)
            except:
                self.logerror("SCP error while downloading dumps, retrying")
                self.catchup[self.time] = newTimeString
                return

        # Load and parse previous files if they are available
        self.logverbose("Loading and parsing the old files")
        old_stats = defaultdict(dict)
        allvdisks, allmdisks = set(), set()
        if not (self.stats_history == self.time - self.interval):
            # Parse the xml files
            for filename in oldDumpsList :
                self.logdebug("Parsing old dump file : {}".format(filename))
                statType, junk1, panelId, junk2, junk3 = filename.split('_')
                old_stats[panelId][statType] = ET.parse('{0}/{1}'.format(dumpsFolder, filename)).getroot()
            # Load relevant xml content in dict 
            for nodeId in nodeEncIdList:
                self.dumps[nodeId] = { 'nodes' : {}, 'mdisks' : {}, 'vdisks' : {}, 'sysid' : '' }
                #Nodes
                if nodeId not in self.dumps[nodeId][nodes] :
                    self.dumps[nodeId][nodes] = { nodeId : {} }
                self.dumps[nodeId][nodes][nodeId]['old'] = {
                    'cpu' : int(old_stats[nodeId]['Nn'].find('{http://ibm.com/storage/management/performance/api/2006/01/nodeStats}cpu').get('busy'))
                }
                #Mdisks
                for mdisk in old_stats[nodeId]['Nm'].findall('{http://ibm.com/storage/management/performance/api/2003/04/diskStats}mdsk'):
                    mdiskId = mdisk.get('id')
                    allmdisks.add(mdiskId)
                    self.dumps[nodeId][mdisks][mdiskId] = {}
                    self.dumps[nodeId][mdisks][mdiskId]['old'] = {
                        'rb' : int(mdisk.get('rb')) * 512,
                        'ro' : int(mdisk.get('ro')),
                        'wb' : int(mdisk.get('wb')) * 512,
                        'wo' : int(mdisk.get('wo')),
                        're' : int(mdisk.get('re')),
                        'we' : int(mdisk.get('we'))
                    }
                #Vdisks
                for vdisk in old_stats[nodeId]['Nv'].findall('{http://ibm.com/storage/management/performance/api/2005/08/vDiskStats}vdsk'):
                    vdiskId = vdisk.get('id')
                    allvdisks.add(vdiskId)
                    self.dumps[nodeId][vdisks][vdiskId] = {}
                    self.dumps[nodeId][vdisks][vdiskId]['old'] = {
                        'ctw' : int(vdisk.get('ctw')), 
                        'ctwwt' : int(vdisk.get('ctwwt')), 
                        'ctwft' : int(vdisk.get('ctwft')), 
                        'rl' : int(vdisk.get('rl')),
                        'wl' : int(vdisk.get('wl')),
                        'rb' : int(vdisk.get('rb')) * 512,
                        'wb' : int(vdisk.get('wb')) * 512, 
                        'ro' : int(vdisk.get('ro')),
                        'wo' : int(vdisk.get('wo'))
                    }
        else: #Transfer new to old, to avoid parsing a file that has already been parsed
            self.logverbose("Old files has already been parsed during previous collect")
            for nodeId in self.dumps:
                for dumpType in  self.dumps[nodeId]:
                    if dumpType != "sysid":
                        for component in self.dumps[nodeId][dumpType]:
                            if 'new' in self.dumps[nodeId][dumpType][component]:
                                self.dumps[nodeId][dumpType][component]['old'] = self.dumps[nodeId][dumpType][component]['new']
                                del self.dumps[nodeId][dumpType][component]['new']

        # Load and parse the current files 
        self.logverbose("Loading and parsing the last files")
        stats = defaultdict(dict)
        dumps = defaultdict(dict)
        downloadedList = str(os.listdir(dumpsFolder))
        self.logdebug("Stats dumps directory contains : \n{}".format(downloadedList))
        for nodeId in nodeEncIdList:
            #Parse the xml files
            for statType in ['Nn', 'Nv', 'Nm']:
                filename = '{0}_stats_{1}_{2}'.format(statType, nodeId, newTimeString)
                if filename not in downloadedList:
                    self.loginfo("Dump not downloaded, could not collect stats : {}".format(filename))
                    self.catchup[self.time] = newTimeString
                    return
                self.logdebug("Parsing dump file : {}".format(filename))
                stats[nodeId][statType] = ET.parse('{0}/{1}'.format(dumpsFolder, filename)).getroot()
            # Load relevant xml content in dict   
            #Nodes
            self.dumps[nodeId]['sysid'] = stats[nodeId]['Nn'].get('id')
            if nodeId not in self.dumps[nodeId][nodes] :
                self.dumps[nodeId][nodes][nodeId] = {}
            self.dumps[nodeId]['nodes'][nodeId]['new'] = {
                'cpu' : int(stats[nodeId]['Nn'].find('{http://ibm.com/storage/management/performance/api/2006/01/nodeStats}cpu').get('busy'))
            }
            #Mdisks
            for mdisk in stats[nodeId]['Nm'].findall('{http://ibm.com/storage/management/performance/api/2003/04/diskStats}mdsk'):
                mdiskId = mdisk.get('id')
                allmdisks.add(mdiskId)
                if mdiskId not in self.dumps[nodeId][mdisks]:
                    self.dumps[nodeId][mdisks][mdiskId] = {}
                self.dumps[nodeId][mdisks][mdisk.get('id')]['new'] = {
                    'rb' : int(mdisk.get('rb')) * 512,
                    'wb' : int(mdisk.get('wb')) * 512,
                    'ro' : int(mdisk.get('ro')),
                    'wo' : int(mdisk.get('wo')),
                    're' : int(mdisk.get('re')),
                    'we' : int(mdisk.get('we')),
                    'pre' : int(mdisk.get('pre')) / 1000,
                    'pwe' : int(mdisk.get('pwe')) / 1000
                }
            #Vdisks
            for vdisk in stats[nodeId]['Nv'].findall('{http://ibm.com/storage/management/performance/api/2005/08/vDiskStats}vdsk'):
                vdiskId = vdisk.get('id')
                allvdisks.add(vdiskId)
                if vdiskId not in self.dumps[nodeId][vdisks]:
                    self.dumps[nodeId][vdisks][vdiskId] = {}
                self.dumps[nodeId][vdisks][vdiskId]['new'] = {
                    'ctw' : int(vdisk.get('ctw')), 
                    'ctwwt' : int(vdisk.get('ctwwt')), 
                    'ctwft' : int(vdisk.get('ctwft')), 
                    'rl' : int(vdisk.get('rl')),
                    'wl' : int(vdisk.get('wl')),
                    'rlw' : int(vdisk.get('rlw')) / 1000,
                    'wlw' : int(vdisk.get('wlw')) / 1000,
                    'rb' : int(vdisk.get('rb')) * 512,
                    'wb' : int(vdisk.get('wb')) * 512, 
                    'ro' : int(vdisk.get('ro')),
                    'wo' : int(vdisk.get('wo'))
                }

            self.logdebug("{} has sysid {}".format(nodeId, self.dumps[nodeId]['sysid']))
        self.logverbose("Finish loading and parsing new files")
        self.stats_history = self.time

        # Remove old stats files
        for filename in dumpsList:
            os.remove('{0}/{1}'.format(dumpsFolder, filename))

        # Load the MdiskGrp names and their Mdisk from the svc cluster
        self.logverbose("Loading the mdisk list")
        mdiskGrpList = { }
        mdiskList = { }
        (success, stdout_mdsk, stderr) = self.check_command('lsmdisk -delim :')
        if not success: return
        isFirst, nameIndex, mdisk_grp_nameIndex = True, -1, -1
        for line in stdout_mdsk:
            splittedLine = line.split(':')
            if isFirst:
                isFirst = False
                nameIndex, mdisk_grp_nameIndex = splittedLine.index('name'), splittedLine.index('mdisk_grp_name')
                continue
            if nameIndex == -1 or mdisk_grp_nameIndex == -1 or nameIndex == mdisk_grp_nameIndex:
                self.loginfo('The first line of the output for \'lsmdisk -delim :\' is missing \'name\' or \'mdisk_grp_name\'')
                self.logverbose("Closing ssh connection")
                self.ssh.close()
                return
            if splittedLine[nameIndex] not in allmdisks: continue
            mdiskList[splittedLine[nameIndex]] = { 
                'mdiskGrpName' : splittedLine[mdisk_grp_nameIndex], 
                'ro' : 0, 
                'wo' : 0, 
                'rrp' : 0, 
                'wrp' : 0 
            }
            mdiskGrpList[splittedLine[mdisk_grp_nameIndex]] = {
                'ro' : 0, 
                'wo' : 0, 
                'rrp' : 0, 
                'wrp' : 0,
                'b_ro' : 0,
                'b_wo' : 0,
                'b_rrp': 0,
                'b_wrp': 0
            }
        for mdisk in allmdisks:
            if mdisk not in mdiskList:
                self.logdebug("Mdisk {} found in dump file is not in lsmdisk".format(mdisk))
                for nodeId in nodeEncIdList:
                    self.dumps[nodeId][mdisks].pop(mdisk, None)
        self.logverbose("Loaded {} entry in the mdisk list".format(len(mdiskList)))

        # Load the vdisk and their mdisk group
        self.logverbose("Loading the vdisk list")
        vdiskList = {}
        manyMdiskgrp = set()
        (success, stdout_vdsk, stderr) = self.check_command('lsvdisk -delim :')
        if not success: return
        isFirst, nameIndex, mdisk_grp_nameIndex = True, -1, -1
        for line in stdout_vdsk:
            splittedLine = line.split(':')
            if isFirst:
                isFirst = False
                nameIndex, mdisk_grp_nameIndex = splittedLine.index('name'), splittedLine.index('mdisk_grp_name')
                continue
            if nameIndex == -1 or mdisk_grp_nameIndex == -1 or nameIndex == mdisk_grp_nameIndex:
                self.loginfo('The first line of the output for \'lsvdisk -delim :\' is missing \'name\' or \'mdisk_grp_name\'')
                self.logverbose("Closing ssh connection")
                self.ssh.close()
                return
            vdiskList[splittedLine[nameIndex]] = { 
                'mdiskGrpName' : '', 
                'ro' : 0, 
                'wo' : 0, 
                'rrp' : 0, 
                'wrp' : 0 
            }
            if splittedLine[mdisk_grp_nameIndex] == 'many': # the vdisk is in several mdisk groups
                manyMdiskgrp.add(splittedLine[nameIndex])
            else: # the vdisk is in a single mdisk group
                vdiskList[splittedLine[nameIndex]]['mdiskGrpName'] = splittedLine[mdisk_grp_nameIndex]

        if(len(manyMdiskgrp) > 0):
            (success, stdout_details, stderr) = self.check_command('lsvdiskcopy -delim :')
            if not success: return
            self.logverbose("{} vdisks on many mdiskGrp, loading details from lsvdiskcopy".format(len(manyMdiskgrp)))
            isFirst, vdisk_nameIndex, mdisk_grp_nameIndex = True, -1, -1
            for line_details in stdout_details:
                splittedLine = line_details.split(':')
                if isFirst: 
                    isFirst = False
                    vdisk_nameIndex, mdisk_grp_nameIndex = splittedLine.index('vdisk_name'), splittedLine.index('mdisk_grp_name')
                    continue
                if vdisk_nameIndex == -1 or mdisk_grp_nameIndex == -1 or nameIndex == mdisk_grp_nameIndex:
                    self.loginfo('The first line of the output for \'lsvdiskcopy -delim :\' is missing \'vdisk_name\' or \'mdisk_grp_name\'')
                    self.logverbose("Closing ssh connection")
                    self.ssh.close()
                    return
                if splittedLine[vdisk_nameIndex] in manyMdiskgrp and vdiskList[splittedLine[vdisk_nameIndex]]['mdiskGrpName'] == '':
                    vdiskList[splittedLine[vdisk_nameIndex]]['mdiskGrpName'] = splittedLine[mdisk_grp_nameIndex]
        for vdisk in allvdisks:
            if vdisk not in vdiskList:
                self.logdebug("Vdisk {} found in dump file is not in lsvdisk".format(vdisk))
                for nodeId in nodeEncIdList:
                    self.dumps[nodeId][vdisks].pop(vdisk, None)
        self.logverbose("Loaded {} entry in the vdisk list".format(len(vdiskList)))
        if self.forcedTime == 0:
            self.logverbose("Closing ssh connection")
            self.ssh.close()

        self.logverbose("Initializing data structures")

        ## Metrics for SVC nodes
        data = { clusternode : {}, clustervdsk : {}, clustermdsk : {} }

        # Initialize the structure for storing the collected data for nodes
        for nodeId in nodeEncIdList:
            data[clusternode][self.dumps[nodeId]['sysid']] = { 'gauge' : {} }
            data[clusternode][self.dumps[nodeId]['sysid']]['gauge'] = {
                'read_response_time' : 0,
                'write_response_time' : 0,
                'backend_read_response_time' : 0,
                'backend_write_response_time' : 0,
                'peak_backend_read_response_time' : 0,
                'peak_backend_write_response_time' : 0,
                'peak_read_response_time' : 0,
                'peak_write_response_time' : 0,
                'write_cache_delay_percentage' : 0,
                'cpu_utilization' : 0,
                'backend_read_data_rate' : 0,
                'backend_read_io_rate' : 0,
                'backend_write_data_rate' : 0,
                'backend_write_io_rate' : 0,
                'read_data_rate' : 0, 
                'read_io_rate' : 0, 
                'write_data_rate' : 0, 
                'write_io_rate' : 0
            }

        # Initialize the structure for storing the collected data for mdisks
        for mdisk in mdiskList:
            data[clustermdsk][mdiskList[mdisk]['mdiskGrpName']] = { 'gauge' : {} }
            data[clustermdsk][mdiskList[mdisk]['mdiskGrpName']]['gauge'] = {
                'backend_read_response_time' : 0,
                'backend_write_response_time' : 0,
                'read_response_time' : 0,
                'write_response_time' : 0,
                'peak_backend_read_response_time' : 0,
                'peak_backend_write_response_time' : 0,
                'peak_read_response_time' : 0,
                'peak_write_response_time' : 0,
                'backend_read_data_rate' : 0,
                'backend_read_io_rate' : 0,
                'backend_write_data_rate' : 0,
                'backend_write_io_rate' : 0,
                'read_data_rate' : 0, 
                'read_io_rate' : 0, 
                'write_data_rate' : 0, 
                'write_io_rate' : 0
            }

        # Initialize the structure for storing the collected data for vdisks
        for vdisk in vdiskList:
            data[clustervdsk][vdisk] = { 'gauge' : {} }
            data[clustervdsk][vdisk]['gauge'] = {
                'read_response_time' : 0, 
                'write_response_time' : 0,
                'peak_read_response_time' : 0, 
                'peak_write_response_time' : 0,
                'read_io_rate' : 0,
                'write_io_rate' : 0,
                'read_data_rate' : 0,
                'write_data_rate' : 0
            }

        self.logverbose("Starting gathering metrics")

        ## Iterate over the nodes to analyse their stats files
        for nodeId in nodeEncIdList:
            node_sysid = self.dumps[nodeId]['sysid']

            # Metrics for nodes (cpu)
            if len(self.dumps[nodeId][nodes][nodeId]) == 2:
                data[clusternode][node_sysid]['gauge']['cpu_utilization'] = (self.dumps[nodeId][nodes][nodeId]['new']['cpu'] - self.dumps[nodeId][nodes][nodeId]['old']['cpu'])/(self.interval * 10) #busy time / total time (milliseconds)

            write_cache_delay_percentage, ctw, ctwft, ctwwt = 0, 0, 0, 0
            total_rrp, total_ro, total_wrp, total_wo = 0, 0, 0, 0
            for vdisk in self.dumps[nodeId][vdisks]:
                if vdisk in vdiskList:
                    mdiskGrp = vdiskList[vdisk]['mdiskGrpName']
                    if len(self.dumps[nodeId][vdisks][vdisk]) == 2:

                        total_ro += self.dumps[nodeId][vdisks][vdisk]['new']['ro'] - self.dumps[nodeId][vdisks][vdisk]['old']['ro']
                        total_wo += self.dumps[nodeId][vdisks][vdisk]['new']['wo'] - self.dumps[nodeId][vdisks][vdisk]['old']['wo']
                        total_rrp += self.dumps[nodeId][vdisks][vdisk]['new']['rl'] - self.dumps[nodeId][vdisks][vdisk]['old']['rl']
                        total_wrp += self.dumps[nodeId][vdisks][vdisk]['new']['wl'] - self.dumps[nodeId][vdisks][vdisk]['old']['wl']

                # Front-end metrics (volumes)    
                        #node
                        data[clusternode][node_sysid]['gauge']['read_data_rate'] += self.dumps[nodeId][vdisks][vdisk]['new']['rb'] - self.dumps[nodeId][vdisks][vdisk]['old']['rb']
                        data[clusternode][node_sysid]['gauge']['read_io_rate'] += self.dumps[nodeId][vdisks][vdisk]['new']['ro'] - self.dumps[nodeId][vdisks][vdisk]['old']['ro']
                        data[clusternode][node_sysid]['gauge']['write_data_rate'] += self.dumps[nodeId][vdisks][vdisk]['new']['wb'] - self.dumps[nodeId][vdisks][vdisk]['old']['wb']
                        data[clusternode][node_sysid]['gauge']['write_io_rate'] += self.dumps[nodeId][vdisks][vdisk]['new']['wo'] - self.dumps[nodeId][vdisks][vdisk]['old']['wo']
                        if data[clusternode][node_sysid]['gauge']['peak_read_response_time'] < self.dumps[nodeId][vdisks][vdisk]['new']['rlw']:
                            data[clusternode][node_sysid]['gauge']['peak_read_response_time'] = self.dumps[nodeId][vdisks][vdisk]['new']['rlw']
                        if data[clusternode][node_sysid]['gauge']['peak_write_response_time'] < self.dumps[nodeId][vdisks][vdisk]['new']['wlw']:
                            data[clusternode][node_sysid]['gauge']['peak_write_response_time'] = self.dumps[nodeId][vdisks][vdisk]['new']['wlw']
                        #mdisk
                        data[clustermdsk][mdiskGrp]['gauge']['read_data_rate'] += self.dumps[nodeId][vdisks][vdisk]['new']['rb'] - self.dumps[nodeId][vdisks][vdisk]['old']['rb']
                        data[clustermdsk][mdiskGrp]['gauge']['read_io_rate'] += self.dumps[nodeId][vdisks][vdisk]['new']['ro'] - self.dumps[nodeId][vdisks][vdisk]['old']['ro']
                        data[clustermdsk][mdiskGrp]['gauge']['write_data_rate'] += self.dumps[nodeId][vdisks][vdisk]['new']['wb'] - self.dumps[nodeId][vdisks][vdisk]['old']['wb']
                        data[clustermdsk][mdiskGrp]['gauge']['write_io_rate'] += self.dumps[nodeId][vdisks][vdisk]['new']['wo'] - self.dumps[nodeId][vdisks][vdisk]['old']['wo']
                        if data[clustermdsk][mdiskGrp]['gauge']['peak_read_response_time'] < self.dumps[nodeId][vdisks][vdisk]['new']['rlw']:
                            data[clustermdsk][mdiskGrp]['gauge']['peak_read_response_time'] = self.dumps[nodeId][vdisks][vdisk]['new']['rlw']
                        if data[clustermdsk][mdiskGrp]['gauge']['peak_write_response_time'] < self.dumps[nodeId][vdisks][vdisk]['new']['wlw']:
                            data[clustermdsk][mdiskGrp]['gauge']['peak_write_response_time'] = self.dumps[nodeId][vdisks][vdisk]['new']['wlw']
                        #vdisk
                        data[clustervdsk][vdisk]['gauge']['read_data_rate'] += self.dumps[nodeId][vdisks][vdisk]['new']['rb'] - self.dumps[nodeId][vdisks][vdisk]['old']['rb']
                        data[clustervdsk][vdisk]['gauge']['read_io_rate'] += self.dumps[nodeId][vdisks][vdisk]['new']['ro'] - self.dumps[nodeId][vdisks][vdisk]['old']['ro']
                        data[clustervdsk][vdisk]['gauge']['write_data_rate'] += self.dumps[nodeId][vdisks][vdisk]['new']['wb'] - self.dumps[nodeId][vdisks][vdisk]['old']['wb']
                        data[clustervdsk][vdisk]['gauge']['write_io_rate'] += self.dumps[nodeId][vdisks][vdisk]['new']['wo'] - self.dumps[nodeId][vdisks][vdisk]['old']['wo']
                        if data[clustervdsk][vdisk]['gauge']['peak_read_response_time'] < self.dumps[nodeId][vdisks][vdisk]['new']['rlw']:
                            data[clustervdsk][vdisk]['gauge']['peak_read_response_time'] = self.dumps[nodeId][vdisks][vdisk]['new']['rlw']
                        if data[clustervdsk][vdisk]['gauge']['peak_write_response_time'] < self.dumps[nodeId][vdisks][vdisk]['new']['wlw']:
                            data[clustervdsk][vdisk]['gauge']['peak_write_response_time'] = self.dumps[nodeId][vdisks][vdisk]['new']['wlw']
                        #Response time
                        vdiskList[vdisk]['ro'] += self.dumps[nodeId][vdisks][vdisk]['new']['ro'] - self.dumps[nodeId][vdisks][vdisk]['old']['ro']
                        vdiskList[vdisk]['wo']  += self.dumps[nodeId][vdisks][vdisk]['new']['wo'] - self.dumps[nodeId][vdisks][vdisk]['old']['wo']
                        vdiskList[vdisk]['rrp']  += self.dumps[nodeId][vdisks][vdisk]['new']['rl'] - self.dumps[nodeId][vdisks][vdisk]['old']['rl']
                        vdiskList[vdisk]['wrp']  += self.dumps[nodeId][vdisks][vdisk]['new']['wl'] - self.dumps[nodeId][vdisks][vdisk]['old']['wl']
                        # write_cache_delay_percentage : Nv file > vdsk > ctwft + ctwwt (flush-through + write through)
                        # write_cache_delay_percentage not possible without accessing previous data, suggest using write_cache_delay_rate
                        ctw += int(self.dumps[nodeId][vdisks][vdisk]['new']['ctw']) - int(self.dumps[nodeId][vdisks][vdisk]['old']['ctw'])
                        ctwft += int(self.dumps[nodeId][vdisks][vdisk]['new']['ctwft']) - int(self.dumps[nodeId][vdisks][vdisk]['old']['ctwft'])
                        ctwwt += int(self.dumps[nodeId][vdisks][vdisk]['new']['ctwwt']) - int(self.dumps[nodeId][vdisks][vdisk]['old']['ctwwt'])

            if ctw > 0:
                write_cache_delay_percentage = ( ctwft + ctwwt ) / ctw
            data[clusternode][node_sysid]['gauge']['write_cache_delay_percentage'] = write_cache_delay_percentage

            if total_ro == 0: #avoid division by 0
                data[clusternode][node_sysid]['gauge']['read_response_time'] = 0
            else :
                data[clusternode][node_sysid]['gauge']['read_response_time'] = float(total_rrp/total_ro)
            if total_wo == 0: #avoid division by 0
                data[clusternode][node_sysid]['gauge']['write_response_time'] = 0
            else :
                data[clusternode][node_sysid]['gauge']['write_response_time'] =float(total_wrp/total_wo)

            # Back-end metrics (disks)
            total_rrp, total_ro, total_wrp, total_wo = 0, 0, 0, 0
            for mdisk in self.dumps[nodeId][mdisks]:
                if mdisk in mdiskList:
                    if len(self.dumps[nodeId][mdisks][mdisk]) == 2:
                        mdiskGrp = mdiskList[mdisk]['mdiskGrpName']
                        #node
                        data[clusternode][node_sysid]['gauge']['backend_read_data_rate'] += self.dumps[nodeId][mdisks][mdisk]['new']['rb'] - self.dumps[nodeId][mdisks][mdisk]['old']['rb']
                        data[clusternode][node_sysid]['gauge']['backend_read_io_rate'] += self.dumps[nodeId][mdisks][mdisk]['new']['ro'] - self.dumps[nodeId][mdisks][mdisk]['old']['ro']
                        data[clusternode][node_sysid]['gauge']['backend_write_data_rate'] += self.dumps[nodeId][mdisks][mdisk]['new']['wb'] - self.dumps[nodeId][mdisks][mdisk]['old']['wb']
                        data[clusternode][node_sysid]['gauge']['backend_write_io_rate'] += self.dumps[nodeId][mdisks][mdisk]['new']['wo'] - self.dumps[nodeId][mdisks][mdisk]['old']['wo']
                        if data[clusternode][node_sysid]['gauge']['peak_backend_read_response_time'] < self.dumps[nodeId][mdisks][mdisk]['new']['pre']:
                            data[clusternode][node_sysid]['gauge']['peak_backend_read_response_time'] = self.dumps[nodeId][mdisks][mdisk]['new']['pre']
                        if data[clusternode][node_sysid]['gauge']['peak_backend_write_response_time'] < self.dumps[nodeId][mdisks][mdisk]['new']['pwe']:
                            data[clusternode][node_sysid]['gauge']['peak_backend_write_response_time'] = self.dumps[nodeId][mdisks][mdisk]['new']['pwe']
                        #mdisk
                        data[clustermdsk][mdiskGrp]['gauge']['backend_read_data_rate'] += self.dumps[nodeId][mdisks][mdisk]['new']['rb'] - self.dumps[nodeId][mdisks][mdisk]['old']['rb']
                        data[clustermdsk][mdiskGrp]['gauge']['backend_read_io_rate'] += self.dumps[nodeId][mdisks][mdisk]['new']['ro'] - self.dumps[nodeId][mdisks][mdisk]['old']['ro']
                        data[clustermdsk][mdiskGrp]['gauge']['backend_write_data_rate'] += self.dumps[nodeId][mdisks][mdisk]['new']['wb'] - self.dumps[nodeId][mdisks][mdisk]['old']['wb']
                        data[clustermdsk][mdiskGrp]['gauge']['backend_write_io_rate'] += self.dumps[nodeId][mdisks][mdisk]['new']['wo'] - self.dumps[nodeId][mdisks][mdisk]['old']['wo']
                        if data[clustermdsk][mdiskGrp]['gauge']['peak_backend_read_response_time'] < self.dumps[nodeId][mdisks][mdisk]['new']['pre']:
                            data[clustermdsk][mdiskGrp]['gauge']['peak_backend_read_response_time'] = self.dumps[nodeId][mdisks][mdisk]['new']['pre']
                        if data[clustermdsk][mdiskGrp]['gauge']['peak_backend_write_response_time'] < self.dumps[nodeId][mdisks][mdisk]['new']['pwe']:
                            data[clustermdsk][mdiskGrp]['gauge']['peak_backend_write_response_time'] = self.dumps[nodeId][mdisks][mdisk]['new']['pwe']
                        #Response time
                        total_ro += self.dumps[nodeId][mdisks][mdisk]['new']['ro'] - self.dumps[nodeId][mdisks][mdisk]['old']['ro']
                        total_wo += self.dumps[nodeId][mdisks][mdisk]['new']['wo'] - self.dumps[nodeId][mdisks][mdisk]['old']['wo']
                        total_rrp += self.dumps[nodeId][mdisks][mdisk]['new']['re'] - self.dumps[nodeId][mdisks][mdisk]['old']['re']
                        total_wrp += self.dumps[nodeId][mdisks][mdisk]['new']['we'] - self.dumps[nodeId][mdisks][mdisk]['old']['we']
                    
                        mdiskList[mdisk]['ro'] += self.dumps[nodeId][mdisks][mdisk]['new']['ro'] - self.dumps[nodeId][mdisks][mdisk]['old']['ro']
                        mdiskList[mdisk]['wo']  += self.dumps[nodeId][mdisks][mdisk]['new']['wo'] - self.dumps[nodeId][mdisks][mdisk]['old']['wo']
                        mdiskList[mdisk]['rrp']  += self.dumps[nodeId][mdisks][mdisk]['new']['re'] - self.dumps[nodeId][mdisks][mdisk]['old']['re']
                        mdiskList[mdisk]['wrp']  += self.dumps[nodeId][mdisks][mdisk]['new']['we'] - self.dumps[nodeId][mdisks][mdisk]['old']['we']

            if total_ro == 0: #avoid division by 0
                data[clusternode][node_sysid]['gauge']['backend_read_response_time'] = 0
            else :
                data[clusternode][node_sysid]['gauge']['backend_read_response_time'] = float(total_rrp/total_ro)
            if total_wo == 0: #avoid division by 0
                data[clusternode][node_sysid]['gauge']['backend_write_response_time'] = 0
            else :
                data[clusternode][node_sysid]['gauge']['backend_write_response_time'] =float(total_wrp/total_wo)


        # Make rates out of counters and remove unnecessary precision
        for node_sysid in data[clusternode]: # node
            data[clusternode][node_sysid]['gauge']['backend_read_data_rate'] = int(data[clusternode][node_sysid]['gauge']['backend_read_data_rate'] / self.interval)
            data[clusternode][node_sysid]['gauge']['backend_read_io_rate'] = int(data[clusternode][node_sysid]['gauge']['backend_read_io_rate'] / self.interval)
            data[clusternode][node_sysid]['gauge']['backend_write_data_rate'] = int(data[clusternode][node_sysid]['gauge']['backend_write_data_rate'] / self.interval)
            data[clusternode][node_sysid]['gauge']['backend_write_io_rate'] = int(data[clusternode][node_sysid]['gauge']['backend_write_io_rate'] / self.interval)
            data[clusternode][node_sysid]['gauge']['read_data_rate'] = int(data[clusternode][node_sysid]['gauge']['read_data_rate'] / self.interval)
            data[clusternode][node_sysid]['gauge']['read_io_rate'] = int(data[clusternode][node_sysid]['gauge']['read_io_rate'] / self.interval)
            data[clusternode][node_sysid]['gauge']['write_data_rate'] = int(data[clusternode][node_sysid]['gauge']['write_data_rate'] / self.interval)
            data[clusternode][node_sysid]['gauge']['write_io_rate'] = int(data[clusternode][node_sysid]['gauge']['write_io_rate'] / self.interval)

        for mdiskGrp in data[clustermdsk]: #mdisk
            data[clustermdsk][mdiskGrp]['gauge']['backend_read_data_rate'] = int( data[clustermdsk][mdiskGrp]['gauge']['backend_read_data_rate'] / self.interval)
            data[clustermdsk][mdiskGrp]['gauge']['backend_read_io_rate'] = int( data[clustermdsk][mdiskGrp]['gauge']['backend_read_io_rate'] / self.interval)
            data[clustermdsk][mdiskGrp]['gauge']['backend_write_data_rate'] = int( data[clustermdsk][mdiskGrp]['gauge']['backend_write_data_rate'] / self.interval)
            data[clustermdsk][mdiskGrp]['gauge']['backend_write_io_rate'] = int( data[clustermdsk][mdiskGrp]['gauge']['backend_write_io_rate'] / self.interval)
            data[clustermdsk][mdiskGrp]['gauge']['read_data_rate'] = int(data[clustermdsk][mdiskGrp]['gauge']['read_data_rate'] / self.interval)
            data[clustermdsk][mdiskGrp]['gauge']['read_io_rate'] = int(data[clustermdsk][mdiskGrp]['gauge']['read_io_rate'] / self.interval)
            data[clustermdsk][mdiskGrp]['gauge']['write_data_rate'] = int(data[clustermdsk][mdiskGrp]['gauge']['write_data_rate'] / self.interval)
            data[clustermdsk][mdiskGrp]['gauge']['write_io_rate'] = int(data[clustermdsk][mdiskGrp]['gauge']['write_io_rate'] / self.interval)

        for vdiskId in data[clustervdsk]: #vdisk
            data[clustervdsk][vdiskId]['gauge']['read_data_rate'] = int(data[clustervdsk][vdiskId]['gauge']['read_data_rate'] / self.interval)
            data[clustervdsk][vdiskId]['gauge']['write_data_rate'] = int(data[clustervdsk][vdiskId]['gauge']['write_data_rate'] / self.interval)
            data[clustervdsk][vdiskId]['gauge']['read_io_rate'] = int(data[clustervdsk][vdiskId]['gauge']['read_io_rate'] / self.interval)
            data[clustervdsk][vdiskId]['gauge']['write_io_rate'] = int(data[clustervdsk][vdiskId]['gauge']['write_io_rate'] / self.interval)

        # Response time
        # Aggregate metrics of individual mdisk by mdiskGrp

        # Frontend latency    
        for vdisk in vdiskList: 
            mdiskGrpList[vdiskList[vdisk]['mdiskGrpName']]['ro'] += vdiskList[vdisk]['ro']
            mdiskGrpList[vdiskList[vdisk]['mdiskGrpName']]['wo'] += vdiskList[vdisk]['wo']
            mdiskGrpList[vdiskList[vdisk]['mdiskGrpName']]['rrp'] += vdiskList[vdisk]['rrp']
            mdiskGrpList[vdiskList[vdisk]['mdiskGrpName']]['wrp'] += vdiskList[vdisk]['wrp']
            if vdiskList[vdisk]['ro'] == 0:
                data[clustervdsk][vdisk]['gauge']['read_response_time'] += float(0)
            else :
                data[clustervdsk][vdisk]['gauge']['read_response_time'] += vdiskList[vdisk]['rrp'] / vdiskList[vdisk]['ro']
            if vdiskList[vdisk]['wo'] == 0:
                data[clustervdsk][vdisk]['gauge']['write_response_time'] += float(0)
            else : 
                data[clustervdsk][vdisk]['gauge']['write_response_time'] += vdiskList[vdisk]['wrp'] / vdiskList[vdisk]['wo']

        # Aggregate metrics of individual mdisk by mdiskGrp
        for mdisk in mdiskList: 
            mdiskGrpList[mdiskList[mdisk]['mdiskGrpName']]['b_ro'] += mdiskList[mdisk]['ro']
            mdiskGrpList[mdiskList[mdisk]['mdiskGrpName']]['b_wo'] += mdiskList[mdisk]['wo']
            mdiskGrpList[mdiskList[mdisk]['mdiskGrpName']]['b_rrp'] += mdiskList[mdisk]['rrp']
            mdiskGrpList[mdiskList[mdisk]['mdiskGrpName']]['b_wrp'] += mdiskList[mdisk]['wrp']

        # Get average response time by IO (total response time / numbers of IO)

        # Backend latency
        for mdiskGrp in mdiskGrpList:
            if mdiskGrpList[mdiskGrp]['b_ro'] == 0: #avoid division by 0
                data[clustermdsk][mdiskGrp]['gauge']['backend_read_response_time'] += float(0)
            else :
                data[clustermdsk][mdiskGrp]['gauge']['backend_read_response_time'] += float(mdiskGrpList[mdiskGrp]['b_rrp']/mdiskGrpList[mdiskGrp]['b_ro'])
            if mdiskGrpList[mdiskGrp]['b_wo'] == 0: #avoid division by 0
                data[clustermdsk][mdiskGrp]['gauge']['backend_write_response_time'] += float(0)
            else :
                data[clustermdsk][mdiskGrp]['gauge']['backend_write_response_time'] += float(mdiskGrpList[mdiskGrp]['b_wrp']/mdiskGrpList[mdiskGrp]['b_wo'])

        # Frontend latency
            if mdiskGrpList[mdiskGrp]['ro'] == 0: #avoid division by 0
                data[clustermdsk][mdiskGrp]['gauge']['read_response_time'] += float(0)
            else :
                data[clustermdsk][mdiskGrp]['gauge']['read_response_time'] += float(mdiskGrpList[mdiskGrp]['rrp']/mdiskGrpList[mdiskGrp]['ro'])
            if mdiskGrpList[mdiskGrp]['wo'] == 0: #avoid division by 0
                data[clustermdsk][mdiskGrp]['gauge']['write_response_time'] += float(0)
            else :
                data[clustermdsk][mdiskGrp]['gauge']['write_response_time'] += float(mdiskGrpList[mdiskGrp]['wrp']/mdiskGrpList[mdiskGrp]['wo'])

        # Set the value to 0 when the counter decrease
        self.logdebug("Changing negative values to 0")
        for level1 in data:
            for level2 in data[level1]:
                for level3 in data[level1][level2]:
                    for level4 in data[level1][level2][level3]:
                        if data[level1][level2][level3][level4] < 0:
                            data[level1][level2][level3][level4] = 0

        # Empty stats in "old" field
        self.logdebug("Emptying old stats")
        for nodeId in self.dumps:
            for dumpType in  self.dumps[nodeId]:
                if dumpType != "sysid":
                    for component in self.dumps[nodeId][dumpType]:
                        if 'old' in self.dumps[nodeId][dumpType][component]:
                            del self.dumps[nodeId][dumpType][component]['old']

        self.logverbose("Finished gathering metrics")
        return data

try:
    plugin = SVCPlugin()
except Exception as exc:
    collectd.error("svc-collect: failed to initialize svc-collect plugin :: %s :: %s"
            % (exc, traceback.format_exc()))

def configure_callback(conf):
    """Received configuration information"""
    plugin.config_callback(conf)
    collectd.register_read(read_callback, plugin.interval)

def read_callback():
    """Callback triggerred by collectd on read"""
    plugin.read_callback()

collectd.register_init(SVCPlugin.reset_sigchld)
collectd.register_config(configure_callback)
