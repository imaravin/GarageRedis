Simple In memory Key value store with HTTP Support

Every slave should be register itself with the master

The master will send data modifying events to the slave.

Replication server will be inited in the next port and the server will receive changes from master

To Run Master

Java Server conf/master.conf

To Run Slave

Java Server conf/slave.conf

Usage

Example

GET

http://localhost:2233/get?key

SET

http://localhost:2233/set?key=value

DEL

http://localhost:2233/del?key

