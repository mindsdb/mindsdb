#!/bin/bash

# First, check if we already ran this script (so we can re-run the same packer on a server, and utilize base builds if desired)
thisscriptname=prepare-mindsdb.sh
if [ -f ~/.$thisscriptname.executed ]; then
    echo "Have already run this script on this server, skipping..."
    exit 0
fi


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
echo ' Setting up Anaconda Environment for MindsDB '
echo '================================================================================'
yes | conda create --name mindsdb python=3.7.1
source activate mindsdb
if [ $? -ne 0 ]; then echo "ERROR: Unable to create mindsdb anaconda environment"; exit 1; fi;
pip install numpy
pip install scipy pandas urllib3 requests pymongo tinymongo torch sklearn
if [ $? -ne 0 ]; then echo "ERROR: Unable to install necessary pip packages"; exit 1; fi;

echo '================================================================================'
echo ' Installing MindsDB... '
echo '================================================================================'
git clone https://github.com/mindsdb/mindsdb.git
cd mindsdb
git checkout $MINDSDB_RELEASE
if [ $? -ne 0 ]; then echo "ERROR: Unable to checkout/use MindsDB from Github or this is an invalid tag/branch/ref"; exit 1; fi;
rm -rf build > /dev/null 2>&1
rm -rf dist  > /dev/null 2>&1
python3 setup.py build
if [ $? -ne 0 ]; then echo "ERROR: Unable to build package dist for MindsDB"; exit 1; fi;
cp -a /home/ubuntu/mindsdb/build/lib/mindsdb /home/ubuntu/anaconda3/envs/mindsdb/lib/python3.7/site-packages
if [ $? -ne 0 ]; then echo "ERROR: Unable to install MindsDB into Anaconda"; exit 1; fi;

echo '================================================================================'
echo ' Setting up CLI Environment to auto-anaconda '
echo '================================================================================'
cd ~/
cp -a mindsdb/LICENSE ./
cp -a mindsdb/*.md ./
cp -a mindsdb/docs ./
rm -Rf mindsdb
echo "source activate mindsdb" >> .bashrc

# We ran successfully, touch this so we don't run this again accidentally on future builds or rebuilds over the same AMI
touch ~/.$thisscriptname.executed
