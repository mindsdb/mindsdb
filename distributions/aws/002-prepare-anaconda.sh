#!/bin/bash

# First, check if we already ran this script (so we can re-run the same packer on a server, and utilize base builds if desired)
thisscriptname=prepare-anaconda.sh
if [ -f ~/.$thisscriptname.executed ]; then
    echo "Have already run this script on this server, skipping..."
    exit 0
fi


echo '================================================================================'
echo ' Installing Anaconda (user-only) '
echo '================================================================================'
wget https://repo.anaconda.com/archive/Anaconda3-5.3.1-Linux-x86_64.sh -O /tmp/anaconda.sh
chmod a+x /tmp/anaconda.sh
# Two hacks to make it install bashrc...
sed -i "s/DEFAULT=no/DEFAULT=yes/" /tmp/anaconda.sh
sed -i '0,/if \[ "$BATCH" = "0" \]; then/ s//if \[ "$BATCH" = "1" \]; then/' /tmp/anaconda.sh
# Hack to make it not fail detection of byte size because of the above modification
sed -i "s/RUNNING_SHELL;/RUNNING_SHELL/" /tmp/anaconda.sh
# Answer yes to prompts and run headless
yes | /tmp/anaconda.sh -b
if [ $? -ne 0 ]; then echo "ERROR: Unable to install Anaconda"; exit 1; fi;


echo '================================================================================'
echo ' Setting up Anaconda CLI Environment '
echo '================================================================================'
export SHLVL=1
export XDG_DATA_DIRS=/usr/local/share:/usr/share:/var/lib/snapd/desktop
export LANG=en_US.utf-8
export LC_ALL=en_US.utf-8
export LC_CTYPE=UTF-8
. /home/ubuntu/.bashrc
export PATH=/home/ubuntu/bin:/home/ubuntu/.local/bin:/home/ubuntu/anaconda3/bin:$PATH


echo '================================================================================'
echo ' Updating Anaconda '
echo '================================================================================'
yes | conda update -n base -c defaults conda
if [ $? -ne 0 ]; then echo "ERROR: Unable to update Anaconda"; exit 1; fi;


# We ran successfully, touch this so we don't run this again accidentally on future builds or rebuilds over the same AMI
touch ~/.$thisscriptname.executed
