# Collect IBM SVC metrics to graphite
This is a collectd plugin developped to collect, calculate and store relevant data to monitor IBM SVC clusters.
The plugin is written with python and we use the write_graphite plugin to store collected data.

## Docker
This collectd plugin work in a dockerized environment : one container is needed for each cluster there is to monitor.
This repository is linked to dockerhub to automatically compile the image corresponding to the last version available here.
The image is available at https://hub.docker.com/r/t4ko/docker-collectd-svc/

## Tags
Additional data like IO_group_name or the mdisk_group_name are associated to metrics. 
We use the tagging capability of Graphite 1.1+ which also allows querying with tags. 


## Running

```
docker run -d \
  -v <Path to your private key>:/svc_privkey \
  -e HOST_NAME=<This container host name> \
  -e GRAPHITE_HOST=<Your graphite host name> \
  -e GRAPHITE_PORT=2003 \
  -e GRAPHITE_PREFIX=<You grapite prefix>. \
  -e PLUGIN_INTERVAL=<Collect interval> \
  -e PLUGIN_CLUSTER_NAME=<The name of the cluster to monitor> \
  -e PLUGIN_CLUSTER_ADDRESS=<The address of the cluster to monitor> \
  -e PLUGIN_CLUSTER_SSHUSER=<Your SVC username> \
  -e PLUGIN_CLUSTER_SSHPRIVKEY=/svc_privkey \
  <Your image>
```


Environment variables:

* `HOST_NAME` - hostname to use in graphite.
* `GRAPHITE_HOST` - host where carbon is listening for data.
* `GRAPHITE_PORT` - port where carbon is listening for data, `2003` by default.
* `GRAPHITE_PREFIX` - prefix for metrics in graphite, `collectd.` by default.
* `PLUGIN_INTERVAL` - collect interval, should match your dump creation interval, `60` by default.
* `PLUGIN_CLUSTER_NAME` - name used for the cluster under which data will be stored.
* `PLUGIN_CLUSTER_ADDRESS` - ip address or domain name of the cluster the container will collect data from.
* `PLUGIN_CLUSTER_SSHUSER` - user name used to connect to the cluster with SSH, `VC` by default.
* `PLUGIN_CLUSTER_SSHPRIVKEY` - path in the container to the private key used to connect with SSH (specified with -v)