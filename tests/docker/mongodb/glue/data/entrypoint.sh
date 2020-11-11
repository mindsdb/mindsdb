#!/bin/bash
echo "INIT TEST DOCKER ENV"
cd /data
/waitforit/wait-for-it.sh 127.0.0.1:27000
/waitforit/wait-for-it.sh 127.0.0.1:27001
mongo --port 27000 -u root -p 123 --authenticationDatabase admin < init_config_rs.js
mongo --port 27000 < init_config_rs.js
mongo --port 27001 < init_db_rs.js
/waitforit/wait-for-it.sh 127.0.0.1:27002
mongo --port 27002 < init_mongos.js
