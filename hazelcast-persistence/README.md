# hazelcast imdg persistence

本项目为 hazelcast imdg 提供了外存支持, 目前支持的结构有:
1. IMap
2. RingBuffer

目前支持的存储引擎有:
1. RocksDB
2. MongoDB

## 示例代码
```
1. 最简单使用, 设置 RocksDB 引擎
Config c = new Config();
PersistenceStorage persistenceStorage = new PersistenceStorage();
persistenceStorage.setStorageMode(StorageMode.RocksDB);
persistenceStorage.initHZConfig(c);

HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);

2. 最简单使用, 设置 MongoDB 引擎
Config c = new Config();
PersistenceStorage persistenceStorage = new PersistenceStorage();
persistenceStorage.setStorageMode(StorageMode.MongoDB);
persistenceStorage.initHZConfig(c);

HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);

3. 完整接口为:
Config c = new Config();
PersistenceStorage persistenceStorage = new PersistenceStorage();
persistenceStorage
    .setStorageMode(StorageMode.MongoDB) // 设置存储引擎, 支持 MongoDB, RocksDB, Mem
    .setDB("cache") // 在存储引擎为 MongoDB 时有效, 设置数据库
    .setCollection("collection") // 在存储引擎为 MongoDB 时有效, 设置数据集合
    .setMongoUri("mongodb://127.0.0.1") // 在存储引擎为 MongoDB 时有效, 设置 mongodb uri
    .setInMemSize(1000) // 设置内存 buffer size
    .setRocksDBPath("./rocksdb-data/"); // 在存储引擎为 RocksDB 时有效, 设置 dbPath
// 其中每个设置, 可以针对 imap 与 ringbuffer 单独设置
persistenceStorage.initHZConfig(c);
HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);
```

## 注意
由于 hazelcast ringBuffer 相关 Store 接口不完善, 实际上不具备可用性, 为保证外存可用, 对 hazelcast 的相关接口做了简单修改

本项目不支持原生 hazelcast, 请使用本项目附带的 hazelcast 进行编译

已经上传 coding 私有仓库, 地址为:
```
<dependency>
<groupId>com.hazelcast</groupId>
<artifactId>hazelcast</artifactId>
<version>5.1-BETA-x-SNAPSHOT</version>
</dependency>
```

## 上传该项目
```shell
cd hazelcast-persistence
mvn clean deploy
```