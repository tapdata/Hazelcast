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

2. 最简答使用, 设置 MongoDB 引擎
Config c = new Config();
PersistenceStorage persistenceStorage = new PersistenceStorage();
persistenceStorage.setStorageMode(StorageMode.MongoDB);
persistenceStorage.initHZConfig(c);

HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);

3. 设置 MongoDB 引擎, 并设置 MongoDB 等信息
Config c = new Config();
PersistenceStorage persistenceStorage = new PersistenceStorage();
persistenceStorage.setStorageMode(StorageMode.MongoDB);
persistenceStorage.setMongoUri("mongodb://127.0.0.1"); // 设置 MongoDB 地址
persistenceStorage.setDB("cache"); // 设置数据库
persistenceStorage.setImapInMemSize(1000); // 设置内存 size
persistenceStorage.initHZConfig(c);

HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);
```

## 注意
由于 hazelcast ringBuffer 相关 Store 接口不完善, 实际上不具备可用性, 为保证外存可用, 对 hazelcast 的相关接口做了简单修改

本项目不支持原生 hazelcast, 请使用本项目附带的 hazelcast 进行编译
