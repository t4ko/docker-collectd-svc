Hostname "{{ HOST_NAME }}"

FQDNLookup false
Timeout 2
ReadThreads 5

LoadPlugin write_graphite

<Plugin "write_graphite">
 <Carbon>
   Host "{{ GRAPHITE_HOST }}"
   Port "{{ GRAPHITE_PORT | default("2003") }}"
   Protocol "tcp"
   Prefix "{{ GRAPHITE_PREFIX | default("svc") }}."
   StoreRates false
   AlwaysAppendDS false
   SeparateInstances true
   PreserveSeparator true
 </Carbon>
</Plugin>

<LoadPlugin "python">
    Globals true
    Interval "{{ PLUGIN_INTERVAL | default("60") }}"
</LoadPlugin>

<Plugin "python">
    ModulePath "/opt/collectd/lib/collectd/plugins"

    Import "svc_plugin"

    <Module "svc_plugin">
        Verbose "{{ PLUGIN_VERBOSE| default("False") }}"
        Debug "{{ PLUGIN_DEBUG | default("False") }}"
        Cluster "{{ PLUGIN_CLUSTER_NAME }}"
        sshAdress = "{{ PLUGIN_CLUSTER_ADDRESS }}"
        sshUser = "{{ PLUGIN_CLUSTER_SSHUSER }}"
        sshRSAkey = "{{ PLUGIN_CLUSTER_SSHPRIVKEY }}"
    </Module>
</Plugin>
