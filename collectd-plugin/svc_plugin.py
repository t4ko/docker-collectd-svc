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
# To add a metric :
# 1- Find the raw data in the dumps
# 2- Add it to the parts parsing the dumps (both 'old' and 'new')
# 3- Add you metric to the structure initialization
# 4- Aggregate the raw data (loops)
# 5- If necessary transform into rate, percentage, etc

import collectd
import json, random, sys, os, time, re
import traceback
import paramiko
from scp import SCPClient
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
        clusternode = "{}.node".format(svc_cluster)
        clustermdskgrp = "{}.mdiskgrp".format(svc_cluster)
        clustervdsk = "{}.vdisk".format(svc_cluster)
        clusterport = "{}.port".format(svc_cluster)
        vdisks, mdisks, nodes, ports = 'vdisks', 'mdisks', 'nodes', 'ports'

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
        if not success: return

        nodes_hash = {}
        config_node =''
        nodeEncIdList = []
        nodeIdList = {}
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
            if not success: return

            nodes_hash[node]['files'] = []

            nodeEncIdList.append(nodes_hash[node]['enclosure_id'])
            nodeIdList[nodes_hash[node]['enclosure_id']] = nodes_hash[node]['name']

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
                    if not success: return

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
            statType, junk, node, day, minute = line.split('_')
            timeString = "{0}_{1}".format(day, minute[:6])
            lsdumpsList.add("{}_stats_{}_{}".format(statType, node, timeString))
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

        #Check if we have the necessary files
        downloadedList = str(os.listdir(dumpsFolder))
        for nodeId in nodeEncIdList:
            #Parse the xml files
            for statType in ['Nn', 'Nv', 'Nm']:
                filename_new = '{0}_stats_{1}_{2}'.format(statType, nodeId, newTimeString)
                filename_old = '{0}_stats_{1}_{2}'.format(statType, nodeId, oldTimeString)
                if filename_new not in downloadedList:
                    self.loginfo("Dump not downloaded, could not collect stats : {}".format(filename_new))
                    self.catchup[self.time] = newTimeString
                    return
                if oldFileAvailable and not oldFileDownloaded:
                    if filename_old not in downloadedList:
                        self.loginfo("Dump not downloaded, could not collect stats : {}".format(filename_old))
                        self.catchup[self.time] = newTimeString
                        return

        # Load and parse previous files if they are available
        self.logverbose("Loading and parsing the old files")
        allvdisks, allmdisks = set(), set()
        if not (self.stats_history == self.time - self.interval):
            # Parse the xml files
            for filename in oldDumpsList :
                self.logdebug("Parsing old dump file : {}".format(filename))
                statType, junk1, nodeId, junk2, junk3 = filename.split('_')
                dumpfile = ET.parse('{0}/{1}'.format(dumpsFolder, filename)).getroot()
                # Load relevant xml content in dict
                if nodeId not in self.dumps: 
                    self.dumps[nodeId] = { nodes : {}, ports: {}, mdisks : {}, vdisks : {}, 'sysid' : '' }

                if statType == "Nn":
                    #Nodes
                    if nodeId not in self.dumps[nodeId][nodes] :
                        self.dumps[nodeId][nodes] = { nodeId : {} }
                    self.dumps[nodeId][nodes][nodeId]['old'] = {
                        'cpu' : int(dumpfile.find('{http://ibm.com/storage/management/performance/api/2006/01/nodeStats}cpu').get('busy'))
                    }
                    #Ports
                    for port in dumpfile.findall('{http://ibm.com/storage/management/performance/api/2006/01/nodeStats}port'):
                        portType = port.get('type')
                        if portType == "FC":
                            portId = "%s_%s" % (nodeIdList[nodeId], port.get('id'))
                            self.dumps[nodeId]['ports'][portId] = {}
                            self.dumps[nodeId]['ports'][portId]['old'] = {
                                'bbcz' : int(port.get('bbcz')),
                                'cbr' : int(port.get('cbr')),
                                'cbt' : int(port.get('cbt')),
                                'cer' : int(port.get('cer')),
                                'cet' : int(port.get('cet')),
                                'hbr' : int(port.get('hbr')),
                                'hbt' : int(port.get('hbt')),
                                'her' : int(port.get('her')),
                                'het' : int(port.get('het')),
                                'icrc' : int(port.get('icrc')),
                                'itw' : int(port.get('itw')),
                                'lf' : int(port.get('lf')),
                                'lnbr' : int(port.get('lnbr')),
                                'lnbt' : int(port.get('lnbt')),
                                'lner' : int(port.get('lner')),
                                'lnet' : int(port.get('lnet')),
                                'lsi' : int(port.get('lsi')),
                                'lsy' : int(port.get('lsy')),
                                'pspe' : int(port.get('pspe')),
                                'rmbr' : int(port.get('rmbr')),
                                'rmbt' : int(port.get('rmbt')),
                                'rmer' : int(port.get('rmer')),
                                'rmet' : int(port.get('rmet')),
                            }
                #Mdisks
                if statType == "Nm":
                    for mdisk in dumpfile.findall('{http://ibm.com/storage/management/performance/api/2003/04/diskStats}mdsk'):
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
                if statType == "Nv":
                    for vdisk in dumpfile.findall('{http://ibm.com/storage/management/performance/api/2005/08/vDiskStats}vdsk'):
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
        dumps = defaultdict(dict)
        downloadedList = str(os.listdir(dumpsFolder))
        self.logdebug("Stats dumps directory contains : \n{}".format(downloadedList))
        for nodeId in nodeEncIdList:
            #Parse the xml files
            for statType in ['Nn', 'Nv', 'Nm']:
                filename = '{0}_stats_{1}_{2}'.format(statType, nodeId, newTimeString)
                self.logdebug("Parsing dump file : {}".format(filename))
                dumpfile = ET.parse('{0}/{1}'.format(dumpsFolder, filename)).getroot()
                # Load relevant xml content in dict   
                if statType == "Nn":
                    #Nodes
                    self.dumps[nodeId]['sysid'] = dumpfile.get('id')
                    if nodeId not in self.dumps[nodeId][nodes] :
                        self.dumps[nodeId][nodes][nodeId] = {}
                    self.dumps[nodeId][nodes][nodeId]['new'] = {
                        'cpu' : int(dumpfile.find('{http://ibm.com/storage/management/performance/api/2006/01/nodeStats}cpu').get('busy'))
                    }
                    #Ports
                    for port in dumpfile.findall('{http://ibm.com/storage/management/performance/api/2006/01/nodeStats}port'):
                        portType = port.get('type')
                        if portType == "FC":
                            portId = "%s_%s" % (nodeIdList[nodeId], port.get('id'))
                            if portId not in self.dumps[nodeId]["ports"]:
                                self.dumps[nodeId]['ports'][portId] = {}
                            self.dumps[nodeId]['ports'][portId]['new'] = {
                                'bbcz' : int(port.get('bbcz')),
                                'cbr' : int(port.get('cbr')),
                                'cbt' : int(port.get('cbt')),
                                'cer' : int(port.get('cer')),
                                'cet' : int(port.get('cet')),
                                'hbr' : int(port.get('hbr')),
                                'hbt' : int(port.get('hbt')),
                                'her' : int(port.get('her')),
                                'het' : int(port.get('het')),
                                'icrc' : int(port.get('icrc')),
                                'itw' : int(port.get('itw')),
                                'lf' : int(port.get('lf')),
                                'lnbr' : int(port.get('lnbr')),
                                'lnbt' : int(port.get('lnbt')),
                                'lner' : int(port.get('lner')),
                                'lnet' : int(port.get('lnet')),
                                'lsi' : int(port.get('lsi')),
                                'lsy' : int(port.get('lsy')),
                                'pspe' : int(port.get('pspe')),
                                'rmbr' : int(port.get('rmbr')),
                                'rmbt' : int(port.get('rmbt')),
                                'rmer' : int(port.get('rmer')),
                                'rmet' : int(port.get('rmet')),
                            }
                #Mdisks
                if statType == "Nm":
                    for mdisk in dumpfile.findall('{http://ibm.com/storage/management/performance/api/2003/04/diskStats}mdsk'):
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
                if statType == "Nv":
                    #Vdisks
                    for vdisk in dumpfile.findall('{http://ibm.com/storage/management/performance/api/2005/08/vDiskStats}vdsk'):
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
            splitted = line.split(':')
            if isFirst:
                isFirst = False
                nameIndex, mdisk_grp_nameIndex, iogrp_nameIndex = splitted.index('name'), splitted.index('mdisk_grp_name'), splitted.index('IO_group_name')
                continue
            if nameIndex == -1 or mdisk_grp_nameIndex == -1 or iogrp_nameIndex == -1:
                self.loginfo('The first line of the output for \'lsvdisk -delim :\' is missing \'name\' or \'mdisk_grp_name\'')
                self.logverbose("Closing ssh connection")
                self.ssh.close()
                return
            vdiskList[splitted[nameIndex]] = { 
                'mdiskGrpName' : '',
                'iogrp': '',
                'ro' : 0, 
                'wo' : 0, 
                'rrp' : 0, 
                'wrp' : 0 
            }
            vdiskList[splitted[nameIndex]]['iogrp'] = splitted[iogrp_nameIndex]
            if splitted[mdisk_grp_nameIndex] == 'many': # the vdisk is in several mdisk groups
                manyMdiskgrp.add(splitted[nameIndex])
            else: # the vdisk is in a single mdisk group
                vdiskList[splitted[nameIndex]]['mdiskGrpName'] = splitted[mdisk_grp_nameIndex]

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
        data = { clusternode : {}, clusterport : {}, clustervdsk : {}, clustermdskgrp : {} }

        # Initialize the structure to store nodes data
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
            # Initialize the structure to store ports data
            for port in self.dumps[nodeId]['ports']:
                if port in data[clusterport]: break
                if 'new' in self.dumps[nodeId]['ports'][port] and 'old' in self.dumps[nodeId]['ports'][port]:
                    data[clusterport][port] = { 'gauge' : {} }
                    data[clusterport][port]['gauge'] = {
                        'disk_receive_data_rate' : 0,
                        'disk_send_data_rate' : 0,
                        'disk_send_io_rate' : 0,
                        'disk_receive_io_rate' : 0,
                        'host_receive_data_rate' : 0,
                        'host_send_data_rate' : 0,
                        'host_send_io_rate' : 0,
                        'host_receive_io_rate' : 0,
                        'lnode_receive_data_rate' : 0,
                        'lnode_send_data_rate' : 0,
                        'lnode_send_io_rate' : 0,
                        'lnode_receive_io_rate' : 0,
                        'rnode_receive_data_rate' : 0,
                        'rnode_send_data_rate' : 0,
                        'rnode_send_io_rate' : 0,
                        'rnode_receive_io_rate' : 0,
                        'invalid_crc_rate': 0,
                        'invalid_word_rate': 0,
                        'link_failure_rate': 0,
                        'pspe_error_rate': 0,
                        'signal_loss_rate': 0,
                        'sync_loss_rate': 0,
                        'zero_buffer_credit_percentage': 0
                    }

        # Initialize the structure to store mdisks data
        for mdisk in mdiskList:
            data[clustermdskgrp][mdiskList[mdisk]['mdiskGrpName']] = { 'gauge' : {} }
            data[clustermdskgrp][mdiskList[mdisk]['mdiskGrpName']]['gauge'] = {
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

        # Initialize the structure to store vdisks data
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
            node_data = data[clusternode][node_sysid]['gauge']

            # Metrics for nodes (cpu)
            if len(self.dumps[nodeId][nodes][nodeId]) == 2:
                node_data['cpu_utilization'] = (self.dumps[nodeId][nodes][nodeId]['new']['cpu'] - self.dumps[nodeId][nodes][nodeId]['old']['cpu'])/(self.interval * 10) #busy time / total time (milliseconds)
            
            for port in self.dumps[nodeId]['ports']:
                if len(self.dumps[nodeId]['ports'][port]) == 2:
                    port_old, port_new = self.dumps[nodeId]['ports'][port]['old'], self.dumps[nodeId]['ports'][port]['new']  # Faster access
                    port_data = data[clusterport][port]['gauge']
                    # Performance metrics
                    port_data['disk_send_data_rate'] += port_new['cbt'] - port_old['cbt']
                    port_data['disk_receive_data_rate'] += port_new['cbr'] - port_old['cbr']
                    port_data['disk_send_io_rate'] += port_new['cet'] - port_old['cet']
                    port_data['disk_receive_io_rate'] += port_new['cer'] - port_old['cer']
                    port_data['host_send_data_rate'] += port_new['hbt'] - port_old['hbt']
                    port_data['host_receive_data_rate'] += port_new['hbr'] - port_old['hbr']
                    port_data['host_send_io_rate'] += port_new['het'] - port_old['het']
                    port_data['host_receive_io_rate'] += port_new['her'] - port_old['her']
                    port_data['lnode_send_data_rate'] += port_new['lnbt'] - port_old['lnbt']
                    port_data['lnode_receive_data_rate'] += port_new['lnbr'] - port_old['lnbr']
                    port_data['lnode_send_io_rate'] += port_new['lnet'] - port_old['lnet']
                    port_data['lnode_receive_io_rate'] += port_new['lner'] - port_old['lner']
                    port_data['rnode_send_data_rate'] += port_new['rmbt'] - port_old['rmbt']
                    port_data['rnode_receive_data_rate'] += port_new['rmbr'] - port_old['rmbr']
                    port_data['rnode_send_io_rate'] += port_new['rmet'] - port_old['rmet']
                    port_data['rnode_receive_io_rate'] += port_new['rmer'] - port_old['rmer']
                    # Error metrics
                    port_data['zero_buffer_credit_percentage'] += port_new['bbcz'] - port_old['bbcz']
                    port_data['invalid_crc_rate'] += port_new['icrc'] - port_old['icrc']
                    port_data['invalid_word_rate'] += port_new['itw'] - port_old['itw']
                    port_data['link_failure_rate'] += port_new['lf'] - port_old['lf']
                    port_data['signal_loss_rate'] += port_new['lsi'] - port_old['lsi']
                    port_data['sync_loss_rate'] += port_new['lsy'] - port_old['lsy']
                    port_data['pspe_error_rate'] += port_new['pspe'] - port_old['pspe']

            write_cache_delay_percentage, ctw, ctwft, ctwwt = 0, 0, 0, 0
            total_rrp, total_ro, total_wrp, total_wo = 0, 0, 0, 0
            for vdisk in self.dumps[nodeId][vdisks]:
                if vdisk in vdiskList:
                    mdiskGrp = vdiskList[vdisk]['mdiskGrpName']
                    if len(self.dumps[nodeId][vdisks][vdisk]) == 2:
                        vdisk_old, vdisk_new = self.dumps[nodeId][vdisks][vdisk]['old'], self.dumps[nodeId][vdisks][vdisk]['new']  # Faster access
                        mdisk_data, vdisk_data = data[clustermdskgrp][mdiskGrp]['gauge'], data[clustervdsk][vdisk]['gauge']

                        total_ro += vdisk_new['ro'] - vdisk_old['ro']
                        total_wo += vdisk_new['wo'] - vdisk_old['wo']
                        total_rrp += vdisk_new['rl'] - vdisk_old['rl']
                        total_wrp += vdisk_new['wl'] - vdisk_old['wl']

                # Front-end metrics (volumes)    
                        #node
                        node_data['read_data_rate'] += vdisk_new['rb'] - vdisk_old['rb']
                        node_data['read_io_rate'] += vdisk_new['ro'] - vdisk_old['ro']
                        node_data['write_data_rate'] += vdisk_new['wb'] - vdisk_old['wb']
                        node_data['write_io_rate'] += vdisk_new['wo'] - vdisk_old['wo']
                        if node_data['peak_read_response_time'] < vdisk_new['rlw']:
                            node_data['peak_read_response_time'] = vdisk_new['rlw']
                        if node_data['peak_write_response_time'] < vdisk_new['wlw']:
                            node_data['peak_write_response_time'] = vdisk_new['wlw']
                        #mdisk
                        mdisk_data['read_data_rate'] += vdisk_new['rb'] - vdisk_old['rb']
                        mdisk_data['read_io_rate'] += vdisk_new['ro'] - vdisk_old['ro']
                        mdisk_data['write_data_rate'] += vdisk_new['wb'] - vdisk_old['wb']
                        mdisk_data['write_io_rate'] += vdisk_new['wo'] - vdisk_old['wo']
                        if mdisk_data['peak_read_response_time'] < vdisk_new['rlw']:
                            mdisk_data['peak_read_response_time'] = vdisk_new['rlw']
                        if mdisk_data['peak_write_response_time'] < vdisk_new['wlw']:
                            mdisk_data['peak_write_response_time'] = vdisk_new['wlw']
                        #vdisk
                        vdisk_data['read_data_rate'] += vdisk_new['rb'] - vdisk_old['rb']
                        vdisk_data['read_io_rate'] += vdisk_new['ro'] - vdisk_old['ro']
                        vdisk_data['write_data_rate'] += vdisk_new['wb'] - vdisk_old['wb']
                        vdisk_data['write_io_rate'] += vdisk_new['wo'] - vdisk_old['wo']
                        if vdisk_data['peak_read_response_time'] < vdisk_new['rlw']:
                            vdisk_data['peak_read_response_time'] = vdisk_new['rlw']
                        if vdisk_data['peak_write_response_time'] < vdisk_new['wlw']:
                            vdisk_data['peak_write_response_time'] = vdisk_new['wlw']
                        #Response time
                        vdiskList[vdisk]['ro'] += vdisk_new['ro'] - vdisk_old['ro']
                        vdiskList[vdisk]['wo']  += vdisk_new['wo'] - vdisk_old['wo']
                        vdiskList[vdisk]['rrp']  += vdisk_new['rl'] - vdisk_old['rl']
                        vdiskList[vdisk]['wrp']  += vdisk_new['wl'] - vdisk_old['wl']
                        # write_cache_delay_percentage : Nv file > vdsk > ctwft + ctwwt (flush-through + write through)
                        # write_cache_delay_percentage not possible without accessing previous data, suggest using write_cache_delay_rate
                        ctw += int(vdisk_new['ctw']) - int(vdisk_old['ctw'])
                        ctwft += int(vdisk_new['ctwft']) - int(vdisk_old['ctwft'])
                        ctwwt += int(vdisk_new['ctwwt']) - int(vdisk_old['ctwwt'])

            if ctw > 0:
                write_cache_delay_percentage = ( ctwft + ctwwt ) / ctw
            node_data['write_cache_delay_percentage'] = write_cache_delay_percentage

            if total_ro == 0: #avoid division by 0
                node_data['read_response_time'] = 0
            else :
                node_data['read_response_time'] = float(total_rrp/total_ro)
            if total_wo == 0: #avoid division by 0
                node_data['write_response_time'] = 0
            else :
                node_data['write_response_time'] =float(total_wrp/total_wo)

            # Back-end metrics (disks)
            total_rrp, total_ro, total_wrp, total_wo = 0, 0, 0, 0
            for mdisk in self.dumps[nodeId][mdisks]:
                if mdisk in mdiskList:
                    if len(self.dumps[nodeId][mdisks][mdisk]) == 2:
                        mdisk_old, mdisk_new = self.dumps[nodeId][mdisks][mdisk]['old'], self.dumps[nodeId][mdisks][mdisk]['new']  # Faster access
                        mdisk_data = data[clustermdskgrp][mdiskGrp]['gauge']
                        mdiskGrp = mdiskList[mdisk]['mdiskGrpName']
                        #node
                        node_data['backend_read_data_rate'] += mdisk_new['rb'] - mdisk_old['rb']
                        node_data['backend_read_io_rate'] += mdisk_new['ro'] - mdisk_old['ro']
                        node_data['backend_write_data_rate'] += mdisk_new['wb'] - mdisk_old['wb']
                        node_data['backend_write_io_rate'] += mdisk_new['wo'] - mdisk_old['wo']
                        if node_data['peak_backend_read_response_time'] < mdisk_new['pre']:
                            node_data['peak_backend_read_response_time'] = mdisk_new['pre']
                        if node_data['peak_backend_write_response_time'] < mdisk_new['pwe']:
                            node_data['peak_backend_write_response_time'] = mdisk_new['pwe']
                        #mdisk
                        mdisk_data['backend_read_data_rate'] += mdisk_new['rb'] - mdisk_old['rb']
                        mdisk_data['backend_read_io_rate'] += mdisk_new['ro'] - mdisk_old['ro']
                        mdisk_data['backend_write_data_rate'] += mdisk_new['wb'] - mdisk_old['wb']
                        mdisk_data['backend_write_io_rate'] += mdisk_new['wo'] - mdisk_old['wo']
                        if mdisk_data['peak_backend_read_response_time'] < mdisk_new['pre']:
                            mdisk_data['peak_backend_read_response_time'] = mdisk_new['pre']
                        if mdisk_data['peak_backend_write_response_time'] < mdisk_new['pwe']:
                            mdisk_data['peak_backend_write_response_time'] = mdisk_new['pwe']
                        #Response time
                        total_ro += mdisk_new['ro'] - mdisk_old['ro']
                        total_wo += mdisk_new['wo'] - mdisk_old['wo']
                        total_rrp += mdisk_new['re'] - mdisk_old['re']
                        total_wrp += mdisk_new['we'] - mdisk_old['we']
                    
                        mdiskList[mdisk]['ro'] += mdisk_new['ro'] - mdisk_old['ro']
                        mdiskList[mdisk]['wo']  += mdisk_new['wo'] - mdisk_old['wo']
                        mdiskList[mdisk]['rrp']  += mdisk_new['re'] - mdisk_old['re']
                        mdiskList[mdisk]['wrp']  += mdisk_new['we'] - mdisk_old['we']

            if total_ro != 0: #avoid division by 0
                node_data['backend_read_response_time'] = float(total_rrp/total_ro)
            if total_wo != 0: #avoid division by 0
                node_data['backend_write_response_time'] =float(total_wrp/total_wo)


        # Make rates out of counters and remove unnecessary precision
        for node_sysid in data[clusternode]: # node
            node_data = data[clusternode][node_sysid]['gauge']
            node_data['backend_read_data_rate'] = int(node_data['backend_read_data_rate'] / self.interval)
            node_data['backend_read_io_rate'] = int(node_data['backend_read_io_rate'] / self.interval)
            node_data['backend_write_data_rate'] = int(node_data['backend_write_data_rate'] / self.interval)
            node_data['backend_write_io_rate'] = int(node_data['backend_write_io_rate'] / self.interval)
            node_data['read_data_rate'] = int(node_data['read_data_rate'] / self.interval)
            node_data['read_io_rate'] = int(node_data['read_io_rate'] / self.interval)
            node_data['write_data_rate'] = int(node_data['write_data_rate'] / self.interval)
            node_data['write_io_rate'] = int(node_data['write_io_rate'] / self.interval)

        for port in data[clusterport]:
            port_data = data[clusterport][port]['gauge']
            # Performance metrics
            port_data['disk_send_data_rate'] = int(port_data['disk_send_data_rate'] / self.interval)
            port_data['disk_receive_data_rate'] = int(port_data['disk_receive_data_rate'] / self.interval)
            port_data['disk_send_io_rate'] = int(port_data['disk_send_io_rate'] / self.interval)
            port_data['disk_receive_io_rate'] = int(port_data['disk_receive_io_rate'] / self.interval)
            port_data['host_send_data_rate'] = int(port_data['host_send_data_rate'] / self.interval)
            port_data['host_receive_data_rate'] = int(port_data['host_receive_data_rate'] / self.interval)
            port_data['host_send_io_rate'] = int(port_data['host_send_io_rate'] / self.interval)
            port_data['host_receive_io_rate'] = int(port_data['host_receive_io_rate'] / self.interval)
            port_data['lnode_send_data_rate'] = int(port_data['lnode_send_data_rate'] / self.interval)
            port_data['lnode_receive_data_rate'] = int(port_data['lnode_receive_data_rate'] / self.interval)
            port_data['lnode_send_io_rate'] = int(port_data['lnode_send_io_rate'] / self.interval)
            port_data['lnode_receive_io_rate'] = int(port_data['lnode_receive_io_rate'] / self.interval)
            port_data['rnode_send_data_rate'] = int(port_data['rnode_send_data_rate'] / self.interval)
            port_data['rnode_receive_data_rate'] = int(port_data['rnode_receive_data_rate'] / self.interval)
            port_data['rnode_send_io_rate'] = int(port_data['rnode_send_io_rate'] / self.interval)
            port_data['rnode_receive_io_rate'] = int(port_data['rnode_receive_io_rate'] / self.interval)
            # Error metrics
            if port_data['zero_buffer_credit_percentage'] > 0:
                port_data['zero_buffer_credit_percentage'] = (self.interval / port_data['zero_buffer_credit_percentage']) // (0.01) / (100) # 0.01 precision
            else:
                port_data['zero_buffer_credit_percentage'] = 0
            port_data['invalid_crc_rate'] = int(port_data['invalid_crc_rate'] / self.interval)
            port_data['invalid_word_rate'] = int(port_data['invalid_word_rate'] / self.interval)
            port_data['link_failure_rate'] = int(port_data['link_failure_rate'] / self.interval)
            port_data['signal_loss_rate'] = int(port_data['signal_loss_rate'] / self.interval)
            port_data['sync_loss_rate'] = int(port_data['sync_loss_rate'] / self.interval)
            port_data['pspe_error_rate'] = int(port_data['pspe_error_rate'] / self.interval)

        for mdiskGrp in data[clustermdskgrp]: #mdisk
            mdisk_data = data[clustermdskgrp][mdiskGrp]['gauge']
            mdisk_data['backend_read_data_rate'] = int( mdisk_data['backend_read_data_rate'] / self.interval)
            mdisk_data['backend_read_io_rate'] = int( mdisk_data['backend_read_io_rate'] / self.interval)
            mdisk_data['backend_write_data_rate'] = int( mdisk_data['backend_write_data_rate'] / self.interval)
            mdisk_data['backend_write_io_rate'] = int( mdisk_data['backend_write_io_rate'] / self.interval)
            mdisk_data['read_data_rate'] = int(mdisk_data['read_data_rate'] / self.interval)
            mdisk_data['read_io_rate'] = int(mdisk_data['read_io_rate'] / self.interval)
            mdisk_data['write_data_rate'] = int(mdisk_data['write_data_rate'] / self.interval)
            mdisk_data['write_io_rate'] = int(mdisk_data['write_io_rate'] / self.interval)

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
            if vdiskList[vdisk]['ro'] != 0:
                data[clustervdsk][vdisk]['gauge']['read_response_time'] += vdiskList[vdisk]['rrp'] / vdiskList[vdisk]['ro']
            if vdiskList[vdisk]['wo'] != 0:
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
            if mdiskGrpList[mdiskGrp]['b_ro'] != 0: #avoid division by 0
                data[clustermdskgrp][mdiskGrp]['gauge']['backend_read_response_time'] += float(mdiskGrpList[mdiskGrp]['b_rrp']/mdiskGrpList[mdiskGrp]['b_ro'])
            if mdiskGrpList[mdiskGrp]['b_wo'] != 0: #avoid division by 0
                data[clustermdskgrp][mdiskGrp]['gauge']['backend_write_response_time'] += float(mdiskGrpList[mdiskGrp]['b_wrp']/mdiskGrpList[mdiskGrp]['b_wo'])

        # Frontend latency
            if mdiskGrpList[mdiskGrp]['ro'] != 0: #avoid division by 0
                data[clustermdskgrp][mdiskGrp]['gauge']['read_response_time'] += float(mdiskGrpList[mdiskGrp]['rrp']/mdiskGrpList[mdiskGrp]['ro'])
            if mdiskGrpList[mdiskGrp]['wo'] != 0: #avoid division by 0
                data[clustermdskgrp][mdiskGrp]['gauge']['write_response_time'] += float(mdiskGrpList[mdiskGrp]['wrp']/mdiskGrpList[mdiskGrp]['wo'])

        # Set the value to 0 when the counter decrease
        self.logdebug("Changing negative values to 0")
        for level1 in data:
            for level2 in data[level1]:
                for level3 in data[level1][level2]:
                    for level4 in data[level1][level2][level3]:
                        if data[level1][level2][level3][level4] < 0:
                            data[level1][level2][level3][level4] = 0

        # WIP : Add tags to metrics (mdisk group, io group)
        for cluster_type in data:
            for equipment in data[cluster_type]:
                #Generate additional tags
                tag_vdsk, tag_port, tag_mdsk, tag_node = "", "", "", "";
                if cluster_type == clustervdsk:
                    tag_vdsk = ";equipment_type=vdisk;IO_grp_name=%s;mdisk_grp_name=%s;cluster=%s" % (
                        vdiskList[equipment]["iogrp"], 
                        vdiskList[equipment]["mdiskGrpName"], 
                        svc_cluster
                    )
                elif cluster_type == clusterport:
                    splitted_port = equipment.split('_')
                    tag_port = ";equipment_type=port;node=%s;port_number=%s;cluster=%s" % (
                        splitted_port[0], 
                        splitted_port[1], 
                        svc_cluster
                    )
                elif cluster_type == clustermdskgrp:
                    tag_mdsk = ";equipment_type=mdiskgrp;cluster=%s" % (
                        svc_cluster
                )
                elif cluster_type == clusternode:
                    tag_node = ";equipment_type=node;cluster=%s" % (
                        svc_cluster
                    )
                data[cluster_type][equipment]['tags'] = "%s%s%s%s" % (tag_node, tag_mdsk, tag_port, tag_vdsk)


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
