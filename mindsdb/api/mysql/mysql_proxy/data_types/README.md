MindsDB toolkit for faking to be a mysql server
---------------


Here we have all the libraries that we need to speak mysql protocol 4.1+

The reference of this implementation is as described in MariaDB documentation
https://mariadb.com/kb/en/library/2-text-protocol/

There are two main objects here,
- the packet which is the unit that is exchanged between the server and client
- the datum, which is the subblocks that a packet is built on

There are various types of packets, which you can find in the packets directory, such as handshake, ok, err msg, etc
Also, we try to keep a catalog of the constants that are needed for this implementation in constants

ENJOY!

NOTE: All of these libraries are homebrewn by mindsdb from scratch,
if yuu need to copy something from a different project, please make sure you place it in external_libs