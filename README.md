This is still in development.


The cache data is stored in jvm offheap memory, which is out of gc managerment, so you can cache huge data and be free of gc pause problem.


For now, to use this library, You need to run your routine on Oracle JDK(SUN JDK). Other jdks may not work properly.
