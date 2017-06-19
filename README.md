# Collect IBM SVC metrics to graphite

blabla

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
* `GRAPHITE_UPDATE_INTERVAL` - metric update interval, `60` by default
* `GRAPHITE_PREFIX` - prefix for metrics in graphite, `collectd.` by default.
