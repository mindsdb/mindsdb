#!/bin/bash

# First, check if we already ran this script (so we can re-run the same packer on a server, and utilize base builds if desired)
thisscriptname=prepare-ubuntu.sh
if [ -f ~/.$thisscriptname.executed ]; then
    echo "Have already run this script on this server, skipping..."
    exit 0
fi

# Now, wait for things to "settle" on the server
echo '================================================================================'
echo ' Starting up and waiting for server to settle (~30 seconds) '
echo '================================================================================'
while [ ! -f "/var/lib/cloud/instance/boot-finished" -a true ]; do
  echo "Waiting for cloud-init to finish..."
  sleep 1
done


# Then update via apt
export DEBIAN_FRONTEND=noninteractive
echo '================================================================================'
echo ' Updating apt '
echo '================================================================================'
sudo ucf --purge /boot/grub/menu.lst
sudo apt-get update
sudo apt-mark hold grub-pc
sudo UCF_FORCE_CONFFNEW=YES apt-get -yq upgrade
# Install base packages
sudo apt-get install -yq build-essential apt-utils virtualenv unzip vim nano \
  software-properties-common byobu curl git htop man ntp python-pip netcat \
  wget joe sysstat jq unzip net-tools telnet iputils-ping awscli
# Error checking...
if [ $? -ne 0 ]; then echo "ERROR: Unable to install ubuntu core packages"; exit 1; fi;
sudo apt-get autoremove -yq
sudo sed -i 's/ -nobackups/-nobackups/g' /etc/joe/joerc

# And add/update some pip packages including pip itslef, and boto3 for aws
sudo pip install boto3 requests awscli pip --upgrade
if [ $? -ne 0 ]; then echo "ERROR: Unable to install pip packages for scripting purposes"; exit 1; fi;

# We ran successfully, touch this so we don't run this again accidentally on future builds or rebuilds over the same AMI
touch ~/.$thisscriptname.executed
