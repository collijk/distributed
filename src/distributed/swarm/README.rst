Swarm
=====

Fully distributed pool of workers with no centralized management.

TODO:

Base Worker
 - Hide centralized monitoring/chaos monkey
 - request capacity from monitor
 - send updates to monitor about topology changes
 - connect to Monitor with REQ socket
 - connect to Chaos Monkey with REQ socket

Monitor service
 - bind REP socket to listen
 - Track network topology
 - track system capacity (maybe let capacity vary dynamically)

Chaos Monkey

