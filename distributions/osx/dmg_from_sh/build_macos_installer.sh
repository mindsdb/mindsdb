#!/bin/bash

echo "This script will build a .dmg file to Install MindsDB on Mac OS"

MINDSDB_VERSION="$(cat src/mindsdb.installer.sh | grep  MINDSDB_VERSION= | tr '=' '\n' | grep -v MINDSDB_VERSION)"


# you will need appdmg to build it
appdmg_path="$(which appdmg)"
if [ -z "${appdmg_path}" ]
then
    echo "Installing appdmg..."
    npm install -g appdmg
    npm install -g fileicon
fi


echo "Cleanup build ..."
rm -rvf build/*


echo "Populating MindsDB Server.app ..."
cp -rv assets/app_template.app build/mindsdb.app
rm -v build/mindsdb.app/Contents/Resources/script
cp -v src/mindsdb.app.sh build/mindsdb.app/Contents/Resources/script
chmod +x build/mindsdb.app/Contents/Resources/script

rm -v build/mindsdb.app/Contents/Resources/mindsdb.installer.sh
cp -v src/mindsdb.installer.sh build/mindsdb.app/Contents/Resources/mindsdb.installer.sh
chmod +x build/mindsdb.app/Contents/Resources/mindsdb.installer.sh

rm -v build/mindsdb.app/Contents/Resources/mindsdb.sh
cp -v src/mindsdb.sh build/mindsdb.app/Contents/Resources/mindsdb.sh
chmod +x build/mindsdb.app/Contents/Resources/mindsdb.sh

# Now we just need to pack the server app in an installer .dmg image
mv build/mindsdb.app "build/MindsDB Server.app"
cp assets/config.json build/config.json
echo "Building .dmg"
cd build
appdmg config.json "MindsDB_v$MINDSDB_VERSION.dmg"
rm -rf "MindsDB Server.app"
rm config.json
fileicon set "MindsDB_v$MINDSDB_VERSION.dmg" ../assets/disk_icon.png
cd ..
