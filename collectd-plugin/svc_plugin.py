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
#   This plugin collects information regarding Ceph pools.
#
# collectd:
#   http://collectd.org
# collectd-python:
#   http://collectd.org/documentation/manpages/collectd-python.5.shtml
# ceph pools:
#   http://ceph.com/docs/master/rados/operations/pools/
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
import subprocess
import xml.etree.cElementTree as ET
from collections import defaultdict

import base

class SVCPlugin(base.Base):

    def __init__(self):
        base.Base.__init__(self)
        self.prefix = 'svc'
        self.ssh = None

    def allowWildcards(self, s):
        """Return a shell-escaped version of the string `s`."""
        _check_safe = re.compile(br'/dumps/iostats/\*[0-9]{6}_[0-9]{6}').search
        if not s:
            return b""
        if _check_safe(s) is None:
            collectd.info("File name does is not a dump file name")
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
                self.logverbose("STDERR : {}".format(errLine))
                if "CMMVC" in errLine: # SVC CLI error
                    commandSuccess = False
            if commandSuccess:
                break
            attempt = attempt - 1 
            time.sleep(1)
        if attempt <= 0 and not commandSuccess:
            collect.info("{} : Command {} failed {} times".format(str(int(time.time())), command, originalAttempt))
            self.ssh.close()
        return commandSuccess, stdout

    def get_stats(self):
        """Retrieves stats from the svc cluster pools"""

        svc_cluster = self.cluster # Defines the name of the current svc cluster (provided in the conf)
        clusternode = "{}.node".format(self.cluster)
        clustermdsk = "{}.mdsk".format(self.cluster)
        clustervdsk = "{}.vdsk".format(self.cluster)

        # Close previous ssh connections if still alive
        if self.ssh is not None:
            transport = self.ssh.get_transport()
            if transport and transport.is_active():
                self.ssh.close()

        self.logverbose("Beginning collection stats")
        # Connect with ssh to svc
        self.logverbose("Connecting with ssh to the cluster")
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
        self.ssh.connect(self.sshAdress, username=self.sshUser, key_filename=self.sshRSAkey, compress=True)
        self.logverbose("Successfuly connected with ssh")

        # Load the node list
        self.logverbose("Loading the node list")
        (success, stdout) = self.check_command('lsnode -delim :')
        if not success: return
        nodeList = set()
        firstLine = True
        enclosure_id_index = 12
        for line in list(stdout):
            if firstLine:
                firstLine = False
                headers = line.split(':')
                enclosure_id_index = headers.index('enclosure_id')
                continue
            nodeInfo = line.split(':')
            nodeList.add(nodeInfo[enclosure_id_index])
        self.logverbose("Loaded {} entry in the node list".format(len(nodeList)))



        # Load the vdisk and their mdisk group
        self.logverbose("Loading the vdisk list")
        vdiskList = {}
        manyMdiskgrp = set()
        (success, stdout_vdsk) = self.check_command('lsvdisk -delim :')
        if not success: return
        isFirst, nameIndex, mdisk_grp_nameIndex = True, -1, -1
        for line in stdout_vdsk:
            splittedLine = line.split(':')
            if isFirst:
                isFirst = False
                nameIndex, mdisk_grp_nameIndex = splittedLine.index('name'), splittedLine.index('mdisk_grp_name')
                continue
            if nameIndex == -1 or mdisk_grp_nameIndex == -1 or nameIndex == mdisk_grp_nameIndex:
                collectd.info('The first line of the output for \'lsvdisk -delim :\' is missing \'name\' or \'mdisk_grp_name\'')
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
            (success, stdout_details) = self.check_command('lsvdiskcopy -delim :')
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
                    collectd.info('The first line of the output for \'lsvdiskcopy -delim :\' is missing \'vdisk_name\' or \'mdisk_grp_name\'')
                    self.ssh.close()
                    return
                if splittedLine[vdisk_nameIndex] in manyMdiskgrp and vdiskList[splittedLine[vdisk_nameIndex]]['mdiskGrpName'] == '':
                    vdiskList[splittedLine[vdisk_nameIndex]]['mdiskGrpName'] = splittedLine[mdisk_grp_nameIndex]
        self.logverbose("Loaded {} entry in the vdisk list".format(len(vdiskList)))

        # Load the MdiskGrp names and their Mdisk from the svc cluster
        self.logverbose("Loading the mdisk list")
        mdiskGrpList = { }
        mdiskList = { }
        (success, stdout_mdsk) = self.check_command('lsmdisk -delim :')
        if not success: return
        isFirst, nameIndex, mdisk_grp_nameIndex = True, -1, -1
        for line in stdout_mdsk:
            splittedLine = line.split(':')
            if isFirst:
                isFirst = False
                nameIndex, mdisk_grp_nameIndex = splittedLine.index('name'), splittedLine.index('mdisk_grp_name')
                continue
            if nameIndex == -1 or mdisk_grp_nameIndex == -1 or nameIndex == mdisk_grp_nameIndex:
                collectd.info('The first line of the output for \'lsmdisk -delim :\' is missing \'name\' or \'mdisk_grp_name\'')
                self.ssh.close()
                return
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
        self.logverbose("Loaded {} entry in the mdisk list".format(len(mdiskList)))










        #Get the time at which all nodes made their iostats dump 
        self.logverbose("Searching the time at which all dumps are available")
        (success, stdout) = self.check_command('lsdumps -prefix /dumps/iostats/')
        if not success: return
        timestamps = {}
        lsdumpsList = set()
        dumpCount = len(nodeList) * 4
        self.logdebug("Lsdumps returns : ")
        for line in reversed(list(stdout)):
            self.logdebug(line.replace('\n', ''))
            if 'id  filename' not in line:
                lsdumpsList.add(line[4:-2])
                junk1, junk2, node, day, minute = line[:-2].split('_')
                timeString = "{0}_{1}".format(day, minute)
                epoch = int(time.mktime(time.strptime(timeString, "%y%m%d_%H%M%S")))
                if epoch in timestamps:
                    timestamps[epoch]['counter'] = timestamps[epoch]['counter'] + 1
                else:
                    timestamps[epoch] = {
                        'string' : timeString,
                        'counter' : 1
                    }
        self.logdebug("lsdumps set contains :\n {}".format(str(lsdumpsList)))
        self.logdebug("Timestamp counter is :\n {}".format(str(timestamps)))
        for epoch in sorted(timestamps.keys(), reverse=True):
            if timestamps[epoch]['counter'] == dumpCount :
                self.time = epoch
                break
        self.logverbose("Timestamp used is {} corresponding string is {}".format(self.time, timestamps[self.time]['string']))
        

        # Compute old timestamp
        newTimeString = timestamps[epoch]['string']
        oldEpoch = self.time - int(self.interval) 
        oldTimeString = time.strftime("%y%m%d_%H%M%S", time.localtime(oldEpoch))

        # Create the dumps directory if it does not exist yet
        dumpsFolder = '{}/svc-stats-dumps'.format(os.getcwd())
        if not os.path.exists(dumpsFolder):
            os.makedirs(dumpsFolder)

        # Check if the last available dumps have not already been collected
        dumpsList = os.listdir(dumpsFolder)
        for dumpName in dumpsList:
            if newTimeString in dumpName:
                collectd.info("New stats dumps are not yet available")
                self.ssh.close()
                return

        # Check if files from previous stats are already in the directory
        useOld = 1
        oldDumpsList = set()
        self.logdebug("Before doing anything the dumps folder contains :\n {}".format(str(dumpsList)))
        for nodeId in nodeList:
            for statType in ['Nn', 'Nv', 'Nm']:
                oldFileName = "{0}_stats_{1}_{2}".format(statType, nodeId, oldTimeString)
                oldDumpsList.add(oldFileName)
                if oldFileName not in dumpsList:
                    useOld = 0

        # Download the file from the SVC cluster if they are available
        self.logverbose("Downloading the dumps with scp")
        t = self.ssh.get_transport()
        scp = SCPClient(t, socket_timeout=30.0, sanitize=self.allowWildcards)


        if useOld == 0:
            useOld = 1
            for oldFileName in oldDumpsList:
                if oldFileName not in lsdumpsList:
                    useOld = 0
            if useOld == 1:
                self.logverbose("Downloading old and new dumps")
                self.logdebug("String passed to scp.get is : /dumps/iostats/*{} /dumps/iostats/*{}".format(oldTimeString, newTimeString))
                scp.get("/dumps/iostats/*{} /dumps/iostats/*{}".format(oldTimeString, newTimeString), dumpsFolder)
        else:
            self.logverbose("Downloading new dumps")
            self.logdebug("String passed to scp.get is : /dumps/iostats/*{}".format(newTimeString))
            scp.get("/dumps/iostats/*{}".format(newTimeString), dumpsFolder)
        self.ssh.close()

        # Load and parse the current files 
        self.logverbose("Loading and parsing the last files")
        stats = defaultdict(dict)
        self.logdebug("Stats dumps directory contains : \n{}".format(str(os.listdir(dumpsFolder))))
        for nodeId in nodeList:
            for statType in ['Nn', 'Nv', 'Nm']:
                filename = '{0}_stats_{1}_{2}'.format(statType, nodeId, newTimeString)
                stats[nodeId][statType] = ET.parse('{0}/{1}'.format(dumpsFolder, filename)).getroot()
            stats[nodeId]['sysid'] = stats[nodeId]['Nn'].get('id')
        self.logverbose("Finish dl and parsing new files")

        # Load and parse previous files if they are available
        if useOld == 1:
            self.logverbose("Loading and parsing the old files")
            old_stats = defaultdict(dict)
            for filename in oldDumpsList :
                statType, junk1, nodeId, junk2, junk3 = filename.split('_')
                old_stats[nodeId][statType] = ET.parse('{0}/{1}'.format(dumpsFolder, filename)).getroot()
            for nodeId in nodeList:
                old_stats[nodeId]['sysid'] = old_stats[nodeId]['Nn'].get('id')


        # Remove old stats files
        for filename in dumpsList:
            os.remove('{0}/{1}'.format(dumpsFolder, filename))










        ## Metrics for SVC nodes
        data = { clusternode : {}, clustervdsk : {}, clustermdsk : {} }
        # Initialize the structure for storing the collected data for nodes
        for nodeId in nodeList:
            data[clusternode][stats[nodeId]['sysid']] = { 'counter' : {}, 'gauge' : {} }
            data[clusternode][stats[nodeId]['sysid']]['counter'] = {
                'cpu_utilization' : 0,
                'read_data_rate' : 0, 
                'read_io_rate' : 0, 
                'write_data_rate' : 0, 
                'write_io_rate' : 0
            }
            data[clusternode][stats[nodeId]['sysid']]['gauge'] = {
                'read_response_time' : 0,
                'write_response_time' : 0,
                'write_cache_delay_percentage' : 0
            }
        # Initialize the structure for storing the collected data for mdisks
        for mdisk in mdiskList:
            data[clustermdsk][mdiskList[mdisk]['mdiskGrpName']] = { 'counter' : {}, 'gauge' : {} }
            data[clustermdsk][mdiskList[mdisk]['mdiskGrpName']]['counter'] = {
                'backend_read_data_rate' : 0,
                'backend_read_io_rate' : 0,
                'backend_write_data_rate' : 0,
                'backend_write_io_rate' : 0,
                'read_data_rate' : 0, 
                'read_io_rate' : 0, 
                'write_data_rate' : 0, 
                'write_io_rate' : 0
            }
            data[clustermdsk][mdiskList[mdisk]['mdiskGrpName']]['gauge'] = {
                'backend_read_response_time' : 0,
                'backend_write_response_time' : 0,
                'read_response_time' : 0,
                'write_response_time' : 0
            }
        # Initialize the structure for storing the collected data for vdisks
        for vdisk in vdiskList:
            data[clustervdsk][vdisk] = { 'counter' : {}, 'gauge' : {} }
            data[clustervdsk][vdisk]['counter'] = {
                'read_io_rate' : 0,
                'write_io_rate' : 0,
                'read_data_rate' : 0,
                'write_data_rate' : 0
            }
            data[clustervdsk][vdisk]['gauge'] = {
                'read_response_time' : 0, 
                'write_response_time' : 0
            }
        










        ## Iterate over the nodes to analyse their stats files
        for nodeId in nodeList:
            node_sysid = stats[nodeId]['sysid'] #for lisibility
            #node_ports = stats[nodeId]['Nn'].findall('{http://ibm.com/storage/management/performance/api/2006/01/nodeStats}port')
            node_vdisks = stats[nodeId]['Nv'].findall('{http://ibm.com/storage/management/performance/api/2005/08/vDiskStats}vdsk')
            node_mdisks = stats[nodeId]['Nm'].findall('{http://ibm.com/storage/management/performance/api/2003/04/diskStats}mdsk')


        # CPU utilization : Nn File > cpu > busy (Extract the counter)
            cpu_utilization = stats[nodeId]['Nn'].find('{http://ibm.com/storage/management/performance/api/2006/01/nodeStats}cpu').get('busy')
            data[clusternode][node_sysid]['counter']['cpu_utilization'] = int(cpu_utilization) 


        # read_data_rate : Nm file > mdsk > rb (512 bytes sector write)
        # read_io_rate : Nm file > mdsk > ro (read operation)
        # write_data_rate : Nm file > mdsk > wb (512 bytes blocks write)
        # write_io_rate : Nm file > mdsk > wo (write operation)
            for mdisk in node_mdisks:
                data[clusternode][node_sysid]['counter']['read_data_rate'] += (int(mdisk.get('rb')) * 512)
                data[clusternode][node_sysid]['counter']['read_io_rate'] += int(mdisk.get('ro'))
                data[clusternode][node_sysid]['counter']['write_data_rate'] += (int(mdisk.get('wb')) * 512)
                data[clusternode][node_sysid]['counter']['write_io_rate'] += int(mdisk.get('wo'))
        

        # read_response_time : Nm file > mdsk > ure (read external response time (microsecond))
        # write_response_time : Nm file > mdsk > uwe (write external response time (microsecond))
            total_rrp, total_ro, total_wrp, total_wo, mdisks_count = 0, 0, 0, 0, len(node_mdisks)
            if useOld == 1:
                old_node_mdisks = set(old_stats[nodeId]['Nm'].findall('{http://ibm.com/storage/management/performance/api/2003/04/diskStats}mdsk'))
                for mdisk in node_mdisks:
                    total_ro += int(mdisk.get('ro'))
                    total_wo += int(mdisk.get('wo'))
                    total_rrp += int(mdisk.get('re'))
                    total_wrp += int(mdisk.get('we'))
                for mdisk in old_node_mdisks:
                    total_ro -= int(mdisk.get('ro'))
                    total_wo -= int(mdisk.get('wo'))
                    total_rrp -= int(mdisk.get('re'))
                    total_wrp -= int(mdisk.get('we'))
                if total_ro == 0: #avoid division by 0
                    data[clusternode][node_sysid]['gauge']['read_response_time'] = 0
                else :
                    data[clusternode][node_sysid]['gauge']['read_response_time'] = float(total_rrp/total_ro)
                if total_wo == 0: #avoid division by 0
                    data[clusternode][node_sysid]['gauge']['write_response_time'] = 0
                else :
                    data[clusternode][node_sysid]['gauge']['write_response_time'] =float(total_wrp/total_wo)


        # write_cache_delay_percentage : Nv file > vdsk > ctwft + ctwwt (flush-through + write through)
        # write_cache_delay_percentage not possible without accessing previous data, suggest using write_cache_delay_rate
            write_cache_delay_percentage, ctw, ctwft, ctwwt = 0, 0, 0, 0
            if useOld == 1:
                for vdisk in node_vdisks:
                    old_vdisk = old_stats[nodeId]['Nv'].find('{0}vdsk[@idx="{1}"]'.format('{http://ibm.com/storage/management/performance/api/2005/08/vDiskStats}', vdisk.get('idx')))
                    if old_vdisk is not None:
                        ctw += int(vdisk.get('ctw')) - int(old_vdisk.get('ctw'))
                        ctwft += int(vdisk.get('ctwft')) - int(old_vdisk.get('ctwft'))
                        ctwwt += int(vdisk.get('ctwwt')) - int(old_vdisk.get('ctwwt'))
                if ctw > 0:
                    write_cache_delay_percentage = ( ctwft + ctwwt ) / ctw
                data[clusternode][node_sysid]['counter']['write_cache_delay_percentage'] = write_cache_delay_percentage









        ## Metrics for MDiskGrp    

        # read_data_rate : Nv file > vdsk > ctrs (512 bytes sector write)
        # read_io_rate : Nv file > vdsk > ro (read operation)
        # write_data_rate : Nv file > vdsk > ctws (512 bytes blocks write)
        # write_io_rate : Nv file > vdsk > wo (write operation)
            for vdisk in node_vdisks:
                vdiskId = vdisk.get('id')
                mdiskGrp = vdiskList[vdiskId]['mdiskGrpName']
                data[clustermdsk][mdiskGrp]['counter']['read_data_rate'] += (int(vdisk.get('rb')) * 512)
                data[clustermdsk][mdiskGrp]['counter']['read_io_rate'] += int(vdisk.get('ro'))
                data[clustermdsk][mdiskGrp]['counter']['write_data_rate'] += (int(vdisk.get('wb')) * 512)
                data[clustermdsk][mdiskGrp]['counter']['write_io_rate'] += int(vdisk.get('wo'))
                if(vdiskId in vdiskList):
                    vdiskList[vdiskId]['ro'] += int(vdisk.get('ro'))
                    vdiskList[vdiskId]['wo']  += int(vdisk.get('wo'))
                    vdiskList[vdiskId]['rrp']  += int(vdisk.get('rl'))
                    vdiskList[vdiskId]['wrp']  += int(vdisk.get('wl'))

        # read_response_time : Nv file > vdsk > rl
        # write_response_time : Nv file > vdsk > wl
            if useOld == 1:
                old_node_vdisks = old_stats[nodeId]['Nv'].findall('{http://ibm.com/storage/management/performance/api/2003/04/diskStats}vdsk')
                for vdisk in old_node_vdisks:
                    vdiskId = vdisk.get('id')
                    if(vdiskId in vdiskList):
                        vdiskList[vdiskId]['ro'] -= int(vdisk.get('ro'))
                        vdiskList[vdiskId]['wo']  -= int(vdisk.get('wo'))
                        vdiskList[vdiskId]['rrp']  -= int(vdisk.get('rl'))
                        vdiskList[vdiskId]['wrp']  -= int(vdisk.get('wl'))


        # backend_read_data_rate : Nm file > mdsk > rb (512 bytes blocks read)
        # backend_read_io_rate : Nm file > mdsk > ro (read operation)
        # backend_write_data_rate : Nm file > mdsk > wb (512 bytes blocks write)
        # backend_write_io_rate : Nm file > mdsk > wo (write operation) 
            for mdisk in node_mdisks:
                mdiskGrp = mdiskList[mdisk.get('id')]['mdiskGrpName']
                data[clustermdsk][mdiskGrp]['counter']['backend_read_data_rate'] += (int(mdisk.get('rb')) * 512)
                data[clustermdsk][mdiskGrp]['counter']['backend_read_io_rate'] += int(mdisk.get('ro'))
                data[clustermdsk][mdiskGrp]['counter']['backend_write_data_rate'] += (int(mdisk.get('wb')) * 512)
                data[clustermdsk][mdiskGrp]['counter']['backend_write_io_rate'] += int(mdisk.get('wo'))
                if(mdisk.get('id') in mdiskList):
                    mdiskList[mdisk.get('id')]['ro'] += int(mdisk.get('ro'))
                    mdiskList[mdisk.get('id')]['wo']  += int(mdisk.get('wo'))
                    mdiskList[mdisk.get('id')]['rrp']  += int(mdisk.get('re'))
                    mdiskList[mdisk.get('id')]['wrp']  += int(mdisk.get('we'))

        # backend_read_response_time
        # backend_write_response_time
            if useOld == 1:
                old_node_mdisks = old_stats[nodeId]['Nm'].findall('{http://ibm.com/storage/management/performance/api/2003/04/diskStats}mdsk')
                for mdisk in old_node_mdisks:
                    if(mdisk.get('id') in mdiskList):
                        mdiskList[mdisk.get('id')]['ro'] -= int(mdisk.get('ro'))
                        mdiskList[mdisk.get('id')]['wo']  -= int(mdisk.get('wo'))
                        mdiskList[mdisk.get('id')]['rrp']  -= int(mdisk.get('re'))
                        mdiskList[mdisk.get('id')]['wrp']  -= int(mdisk.get('we'))










        ## Metrics for vdisks
        # read_io_rate Nv > vdsk > ro
        # write_io_rate Nv > vdsk > wo
        # read_data_rate Nv > vdsk > wo
        # write_data_rate Nv > vdsk > wo
        # read_response_time : Nv file > vdsk > rl
        # write_response_time : Nv file > vdsk > wl
            for vdisk in node_vdisks:
                data[clustervdsk][vdiskId]['counter']['read_data_rate'] += int(vdisk.get('rb'))
                data[clustervdsk][vdiskId]['counter']['write_data_rate'] += int(vdisk.get('wb'))
                data[clustervdsk][vdiskId]['counter']['read_io_rate'] += int(vdisk.get('ro'))
                data[clustervdsk][vdiskId]['counter']['write_io_rate'] += int(vdisk.get('wo'))










        # Frontend latency
        # Aggregate metrics of individual mdisks by mdiskGrp
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

        # Get average response time by IO (total response time / numbers of IO)
        for mdiskGrp in mdiskGrpList:
            if mdiskGrpList[mdiskGrp]['ro'] == 0: #avoid division by 0
                data[clustermdsk][mdiskGrp]['gauge']['read_response_time'] += float(0)
            else :
                data[clustermdsk][mdiskGrp]['gauge']['read_response_time'] += float(mdiskGrpList[mdiskGrp]['rrp']/mdiskGrpList[mdiskGrp]['ro'])
            if mdiskGrpList[mdiskGrp]['wo'] == 0: #avoid division by 0
                data[clustermdsk][mdiskGrp]['gauge']['write_response_time'] += float(0)
            else :
                data[clustermdsk][mdiskGrp]['gauge']['write_response_time'] += float(mdiskGrpList[mdiskGrp]['wrp']/mdiskGrpList[mdiskGrp]['wo'])


        # Backend latency
        # Aggregate metrics of individual mdisks by mdiskGrg
        for mdisk in mdiskList: 
            mdiskGrpList[mdiskList[mdisk]['mdiskGrpName']]['b_ro'] += mdiskList[mdisk]['ro']
            mdiskGrpList[mdiskList[mdisk]['mdiskGrpName']]['b_wo'] += mdiskList[mdisk]['wo']
            mdiskGrpList[mdiskList[mdisk]['mdiskGrpName']]['b_rrp'] += mdiskList[mdisk]['rrp']
            mdiskGrpList[mdiskList[mdisk]['mdiskGrpName']]['b_wrp'] += mdiskList[mdisk]['wrp']
        # Get average response time by IO (total response time / numbers of IO)
        for mdiskGrp in mdiskGrpList:
            if mdiskGrpList[mdiskGrp]['b_ro'] == 0: #avoid division by 0
                data[clustermdsk][mdiskGrp]['gauge']['backend_read_response_time'] += float(0)
            else :
                data[clustermdsk][mdiskGrp]['gauge']['backend_read_response_time'] += float(mdiskGrpList[mdiskGrp]['b_rrp']/mdiskGrpList[mdiskGrp]['b_ro'])
            if mdiskGrpList[mdiskGrp]['b_wo'] == 0: #avoid division by 0
                data[clustermdsk][mdiskGrp]['gauge']['backend_write_response_time'] += float(0)
            else :
                data[clustermdsk][mdiskGrp]['gauge']['backend_write_response_time'] += float(mdiskGrpList[mdiskGrp]['b_wrp']/mdiskGrpList[mdiskGrp]['b_wo'])










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