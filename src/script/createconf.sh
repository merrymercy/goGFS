#!/bin/bash
id=$(ifconfig eth0 | grep -Eo '192.168.2.1([0-9]+)' | sed 's/192.168.2.1//g')
echo "id=cs$id" > ~/conf
echo "role=chunkserver" >> ~/conf
echo "listen=192.168.2.1$id:10000" >> ~/conf
echo "master=192.168.2.127:7777" >> ~/conf
echo "center=192.168.2.128:6666" >> ~/conf
echo "eth=eth0" >> ~/conf

