The cache data is stored in jvm direct memory, which is off-heap and out of gc management, so you can cache huge data and be free of gc pause problem.

To use this library, You need to run your routine on Oracle JDK(SUN JDK). The cache value should be large enough (> 1k, for example), so the direct cache will really heap

When get a cache instance, should always specify the max off-heap memory that cache can use:
```
 DirectCache<String, String> cache = DirectCache.<String, String>newBuilder().maxMemorySize(Size.Gb(10)).build();
```

For maven users, add:
```xml
<dependency>
    <groupId>net.dongliu</groupId>
    <artifactId>direct-cache</artifactId>
    <version>0.2.5</version>
</dependency>
```
to your pom file.
