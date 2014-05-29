The cache data is stored in jvm direct memory, which is off-heap and out of gc management, so you can cache huge data and be free of gc pause problem.

To use this library, You need to run your routine on Oracle JDK(SUN JDK). The cache value should be large enough (> 1k, for example), so the direct cache will really heap

Get a cache instance:
```
 DirectCache cache = DirectCache.newBuilder().maxMemorySize(Size.Gb(10)).build();
```