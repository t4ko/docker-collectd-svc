FROM ubuntu:16.04

ENV DEBIAN_FRONTEND noninteractive 

# Install all prerequisites for building collectd, paramiko and python plugin
RUN apt-get -y update && apt-get -y install wget libssl-dev libffi-dev build-essential python3-dev python3-pip ssh tzdata git
RUN pip3 install envtpl paramiko scp

# Get and untar sources files 
#RUN wget https://collectd.org/files/collectd-5.7.2.tar.bz2
#RUN tar jxvf collectd-5.7.2.tar.bz2 && rm collectd-5.7.2.tar.bz2
#Use git until the pull request for tags in write_graphite plugin is merged
RUN git clone https://github.com/DanCech/collectd.git -b tagged-carbon ./collectd-src
RUN cd collectd-src
RUN git checkout 09666a4a1d3511cbc6c4473f8946bd334a80d55b


# Compile and purge source files 
RUN ./configure && make all install
RUN cd .. && rm -rf collectd-src
#RUN cd collectd-5.7.2 && ./configure && make all install
#RUN cd .. && rm -rf collectd-5.7.2

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