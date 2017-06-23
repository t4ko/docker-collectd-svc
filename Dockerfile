FROM ubuntu:16.04

ENV DEBIAN_FRONTEND noninteractive 

# Install all prerequisites for building collectd, paramiko and python plugin
RUN apt-get -y update && apt-get -y install wget libssl-dev libffi-dev build-essential python3-dev python3-pip ssh
RUN pip3 install envtpl paramiko scp

# Get and untar sources files 
RUN wget https://collectd.org/files/collectd-5.7.2.tar.bz2
RUN tar jxvf collectd-5.7.2.tar.bz2 && rm collectd-5.7.2.tar.bz2

# Compile and purge source files 
RUN cd collectd-5.7.2 && ./configure && make all install
RUN cd .. && rm -rf collectd-5.7.2

# Optionnal post installation tasks 
RUN ln -s /opt/collectd/sbin/collectd /usr/sbin/collectd
RUN ln -s /opt/collectd/sbin/collectdmon /usr/sbin/collectdmon
RUN apt-get clean
RUN apt-get purge

# Deploy collectd plugin and config template
ADD collectd-plugin/base.py /opt/collectd/lib/collectd/plugins/
ADD collectd-plugin/svc_plugin.py /opt/collectd/lib/collectd/plugins/
ADD collectd.conf.tpl /opt/collectd/etc/collectd.conf.tpl

ADD ./run.sh /run.sh
ENTRYPOINT ["/run.sh"]