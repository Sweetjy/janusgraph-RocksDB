# janusgraph-RocksDB
将 RocksDB 接入到 JanusGraph

#### 代码说明
1.janusgraph-rocksdb 是核心代码，其中 RocksDBConfig 是 rocksdb 的一些配置，可以在properties文件中进行设置

如```storeage.rocksdb.Bits_per_key=8```是指修改Bits_per_key的配置参数 

2.janusgraph-rocksdb-lucene.properties 是使用rokcsdb 作为存储引擎，lucene作为索引引擎的配置文件示例，Cancel changes

3.janusgraph-core中仅在  ```StandardStoreManager.java```,```Backend.java```,```ReflectiveConfigOptionLoader.java```中添加了关于rocksdb的一些代码。

4.运行janusgraph-test/src/test/mytest/JanusGraphFirstTest.java 可以对代码进行测试

#### 使用
1.直接下载运行

2.将janusgraph-rocksdb作为包添加到 janusgraph 源码中使用，在janusgraph-core中添加相关内容，将properties文件放到janusgraph-dist\src\assembly\cfilter\conf目录下
