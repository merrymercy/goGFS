# goGFS
goGFS is a simple implementation of the Google File System (GFS) in golang.  
It is tested with a small cluster of inexpensive machines (1 master, 12 chunkservers and 12 clients).  
Fault tolerance is provided.

# Feature
* Architecture & Consistency Model
    * Same as the paper
* System Interactions
    * atomic record append (append at least once)
* Master
    * Persistent Metadata
    * Re-replication
    * Garbage Collection
    * Stale Detection
* ChunkServer
    * Persistent Metadata
* Client
    * Familiar File System Interface
* Fault Tolerance

# Todo
* pipelined data flow
* snapshot

# Reference
[The Google File System](http://research.google.com/archive/gfs.html)  
[Class Wiki](http://acm.sjtu.edu.cn/wiki/PPCA_2016#.E5.88.86.E5.B8.83.E5.BC.8F.E7.B3.BB.E7.BB.9F)  
[Test Repo](https://bitbucket.org/abcdabcd987/ppca-gfs)
