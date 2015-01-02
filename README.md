The cache data is stored in jvm direct memory, which is off-heap and out of gc management, so you can cache huge data and be free of gc pause problem.

The cache value should be large enough (> 512Byte, for example), so the direct cache will really help

When get a cache instance, should always specify the max off-heap memory that cache can use:
```java
 DirectCache cache = DirectCache.newBuilder().maxMemorySize(Size.Gb(100)).build();
```

For maven users, add:
```xml
<dependency>
    <groupId>net.dongliu</groupId>
    <artifactId>direct-cache</artifactId>
    <version>0.4.2</version>
</dependency>
```
to your pom file.
