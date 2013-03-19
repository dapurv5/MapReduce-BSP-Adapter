MapReduce-BSP-Adapter
=====================

Simple implementation of Map-Reduce over Apache Hama BSP framework. For key ideas please refer to http://javahadoopalgorithms.blogspot.in/2013/01/map-reduce-over-bulk-synchronous.html

### QUICKSTART
If you have written a map-reduce job you'll find the api very similar.
You can find a sample WordCount example here. https://github.com/dapurv5/MapReduce-BSP-Adapter/blob/master/src/main/java/org/apache/hama/mapreduce/examples/WordCount.java

### KNOWN BUGS
Atmost one word being sent to both the peers. Either caused due to some portions of input being duplicated while partitioning into splits or due to error in loopback messaging in Hama.
