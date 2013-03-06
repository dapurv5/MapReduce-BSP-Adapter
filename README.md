MapReduce-BSP-Adapter
=====================

Simple implementation of Map-Reduce over Apache Hama BSP framework. For key ideas please refer to http://javahadoopalgorithms.blogspot.in/2013/01/map-reduce-over-bulk-synchronous.html


### KNOWN BUGS
Atmost one word being sent to both the peers. Either caused due to some portions of input being duplicated while partitioning into splits or due to error in loopback messaging in Hama.
