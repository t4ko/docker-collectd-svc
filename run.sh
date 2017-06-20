#!/bin/bash

set -e

if [ ! -e "/.initialized" ]; then
    touch "/.initialized"
    envtpl /opt/collectd/etc/collectd.conf.tpl
fi

exec collectd -f