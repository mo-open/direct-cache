Cache using java direct memory. The cache data is in offheap memory, which is out of jvm gc managerment, so you can cache huge data in java process and be free of gc pause problem.


To use this library, You need to use a openJDK or oracle JDK, other jdks may not work properly.
