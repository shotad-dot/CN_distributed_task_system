#!/bin/bash
sudo mn -c
sudo ovs-vsctl --if-exists del-port s1 host0
