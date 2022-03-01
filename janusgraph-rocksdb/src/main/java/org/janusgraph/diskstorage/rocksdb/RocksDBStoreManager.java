// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package org.janusgraph.diskstorage.rocksdb;

import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.common.LocalStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.janusgraph.util.system.IOUtils;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.janusgraph.diskstorage.rocksdb.RocksDBConfig.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY;

/**
 * @author Sweetjy
 * @date 2021/11/23
 */
@PreInitializeConfigOptions
public class RocksDBStoreManager extends LocalStoreManager implements OrderedKeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(RocksDBStoreManager.class);

    //由参数传过来的数据文件的存储路径
    private static String rocksDbDir;
//    private static RocksDB rocksDb = null;
    private static OptimisticTransactionDB rocksDb;
//    private static int references = 0;


    Options options;
    DBOptions dbOptions;

    //column family相关成员变量
//    private List<byte[]> cfNames = new ArrayList<>();
    List<byte[]> cfNames = new ArrayList<>();
    //用于创建或删除一个column family
    private final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
    private final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    //用于操作一个column family
//    private final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
//    private ConcurrentMap<String, ColumnFamilyHandle> cfHandleMap = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, ColumnFamily> COLUMN_FAMILIES = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, Lock> COLUMN_FAMILY_LOCKS = new ConcurrentHashMap<>();


    //图数据会被划分成多个列族，每个列族表示一组相关的数据，比如边、顶点等
    private final Map<String, RocksDBKeyColumnValueStore> stores;

    protected StoreFeatures features;

    //构造函数，初始化存储store的队列，定义该storage backend支持的图数据库特性
    public RocksDBStoreManager(Configuration config) throws BackendException {
        super(config);
//        this.tableName = config.get(GRAPH_NAME);
        stores = new ConcurrentHashMap<>();

        //打开rocksdb
        rocksDbDir = config.get(STORAGE_DIRECTORY);
//        log.debug("Opened the database from {}",rocksDbDir);
        File dir = new File(rocksDbDir);
        if(!dir.exists())
            dir.mkdirs();

        BlockBasedTableConfig bbOption = new BlockBasedTableConfig()
            .setFilterPolicy(new BloomFilter(config.get(BLOOM_FILTER)));

        //rocksdb的配置选项
        options = new Options()
            .setWriteBufferSize(config.get(ROCKSDB_WRITE_BUFFER))
            .setLevel0FileNumCompactionTrigger(config.get(LEVEL0_NUM_COMPACTION_TRIGGER))
            .setTargetFileSizeBase(config.get(TARGET_FILE_SIZE_BASE))
            .setMaxBytesForLevelBase(config.get(MAX_BYTES_FOR_LEVEL_BASE))
            .setCreateIfMissing(true)
            .setTableFormatConfig(bbOption)
            .setInfoLogLevel(InfoLogLevel.INFO_LEVEL);

        cfOptions.setWriteBufferSize(config.get(ROCKSDB_WRITE_BUFFER))
            .setLevel0FileNumCompactionTrigger(config.get(LEVEL0_NUM_COMPACTION_TRIGGER))
            .setTargetFileSizeBase(config.get(TARGET_FILE_SIZE_BASE))
            .setMaxBytesForLevelBase(config.get(MAX_BYTES_FOR_LEVEL_BASE))
            .setTableFormatConfig(bbOption);

        dbOptions = new DBOptions()
            .setCreateIfMissing(true);

        if(config.get(ROCKSDB_SMALLDB)) {
            dbOptions.optimizeForSmallDb();
            options.optimizeForSmallDb();
        }
    }

    private final class ColumnFamily {
        private final ColumnFamilyHandle handle;
        private final ColumnFamilyOptions options;

        private ColumnFamily(final ColumnFamilyHandle handle, final ColumnFamilyOptions options) {
            this.handle = handle;
            this.options = options;
        }

        public ColumnFamilyHandle getHandle() {
            return handle;
        }

        public ColumnFamilyOptions getOptions() {
            return options;
        }
    }

    private void createColumnFamily(final String name) throws RocksDBException {
        COLUMN_FAMILY_LOCKS.putIfAbsent(name, new ReentrantLock());

        final Lock l = COLUMN_FAMILY_LOCKS.get(name);
        l.lock();
        try {
            if(!COLUMN_FAMILIES.containsKey(name)) {
                final ColumnFamilyHandle cfHandle = rocksDb.createColumnFamily(
                    new ColumnFamilyDescriptor(name.getBytes(UTF_8), cfOptions)
                );
                COLUMN_FAMILIES.put(name, new ColumnFamily(cfHandle, cfOptions));

            }
        } finally {
            l.unlock();
        }
    }

    @Override
    public RocksDBKeyColumnValueStore openDatabase(String name) {
        if (stores.containsKey(name)) {
            return stores.get(name);
        }


        try {
            cfNames = RocksDB.listColumnFamilies(options, rocksDbDir);
            for (byte[] cf : cfNames) {
                cfDescriptors.add(new ColumnFamilyDescriptor(cf, cfOptions));
            }

            if(cfDescriptors.isEmpty()){
                rocksDb = OptimisticTransactionDB.open(options, rocksDbDir);
            } else if(rocksDb == null){
                final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
                rocksDb = OptimisticTransactionDB.open(dbOptions, rocksDbDir, cfDescriptors, cfHandles);
                for(int i = 0; i < cfNames.size(); i++) {
                    COLUMN_FAMILIES.put(new String(cfNames.get(i)), new ColumnFamily(cfHandles.get(i), cfOptions));
//                    ColumnFamilyHandle cfHandle = rocksDb.createColumnFamily(
//                        new ColumnFamilyDescriptor(name.getBytes(), new ColumnFamilyOptions()));
//                        cfHandleMap.put(name, cfHandle);
                }
            }
            if(!COLUMN_FAMILIES.containsKey(name))
                createColumnFamily(name);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(name).getHandle();

        //name是一个列簇的名字
//        RocksDBKeyColumnValueStore store =
//            new RocksDBKeyColumnValueStore(name, rocksDb, cfHandleMap.get(name), this);
        RocksDBKeyColumnValueStore store =
            new RocksDBKeyColumnValueStore(name, rocksDb, cf, this);
        //一个图会分割成多个store来存储不同的信息，这里的那么指的就是store的名字，比如存储配置参数的store，edge的store等
        stores.put(name, store);
//        log.debug("column family is {}, store size is {}", name,stores.size());
        return store;
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException {
        for (Map.Entry<String,KVMutation> mutation : mutations.entrySet()) {
            //key是store的名字，也就是column family的名字
            RocksDBKeyColumnValueStore store = openDatabase(mutation.getKey());
            KVMutation mutationValue = mutation.getValue();

            if (!mutationValue.hasAdditions() && !mutationValue.hasDeletions()) {
                log.debug("Empty mutation set for {}, doing nothing", mutation.getKey());
            } else {
                log.debug("Mutating {}", mutation.getKey());
            }

            if (mutationValue.hasAdditions()) {
                for (KeyValueEntry entry : mutationValue.getAdditions()) {
                    store.insert(entry.getKey(),entry.getValue(),txh, entry.getTtl());
//                    log.trace("Insertion on {}: {}", mutation.getKey(), entry);
                }
            }
            if (mutationValue.hasDeletions()) {
                for (StaticBuffer del : mutationValue.getDeletions()) {
                    store.delete(del,txh);
//                    log.trace("Deletion on {}: {}", mutation.getKey(), del);
                }
            }
        }
    }


    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        Transaction transaction = rocksDb.beginTransaction(new WriteOptions());
        return new RocksdbTx(config, transaction);
    }

    @Override
    public void close() throws BackendException {
        stores.clear();
        synchronized (RocksDBStoreManager.class) {
            try {
                for (final ColumnFamily cf : COLUMN_FAMILIES.values()) {
                    cf.getHandle().close();
                }
                rocksDb.close();
                rocksDb = null;
                dbOptions.close();
                dbOptions = null;

            } catch (Exception e) {
                throw new PermanentBackendException(e);
            }
        }
        log.info("RocksDB has bean closed");
    }

    @Override
    public void clearStorage(){
        stores.clear();
//        cfHandleMap.clear();
        COLUMN_FAMILIES.clear();
        rocksDb.close();
        rocksDb = null;
        IOUtils.deleteFromDirectory(new File(rocksDbDir));
        log.info("RocksDBStoreManager cleared storage");
    }

    @Override
    public boolean exists() {
//        return !cfHandleMap.isEmpty();
        return !COLUMN_FAMILIES.isEmpty();
    }

    @Override
    public StoreFeatures getFeatures() {
        //janusgraph-core来初始化一个支持这些特性的图数据库
        features = new StandardStoreFeatures.Builder()
            .orderedScan(true)
            .multiQuery(true)
            .batchMutation(true)
            .keyOrdered(true)
            .transactional(transactional)
            .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
            .locking(true)
            .supportsInterruption(false)
            .build();
        return features;
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }
}
