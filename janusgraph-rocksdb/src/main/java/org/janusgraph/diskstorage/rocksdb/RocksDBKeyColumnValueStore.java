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

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.mozilla.universalchardet.UniversalDetector;
import org.rocksdb.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ValueExp;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author Sweetjy
 * @date 2021/11/23
 */
public class RocksDBKeyColumnValueStore implements OrderedKeyValueStore {
    private static final Logger log = LoggerFactory.getLogger(RocksDBKeyColumnValueStore.class);

    private final RocksDB rocksdb;
    private final RocksDBStoreManager manager;
    private final String cfName;
    private final ColumnFamilyHandle cfHandle;

    public RocksDBKeyColumnValueStore(String name, RocksDB rocksdb, ColumnFamilyHandle handle, RocksDBStoreManager rocksDBStoreManager) {
        this.rocksdb = rocksdb;
        this.manager = rocksDBStoreManager;
        this.cfName = name;
        this.cfHandle = handle;
   }

    /*
    读取RocksDB中存的数据
    数据库启动之初，会读取system_properties（store）的数据，这里面是一些配置信息，需要用来初始化janusgraph
    之后根据请求的内容，访问不同数据表。基本上访问边、顶点、相关属性会用到edgestore、graphindex
    */
    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException {
//        log.info("beginning db={}, op=getSlice, tx={}, from {} to {}", cfName, txh, query.getStart(), query.getEnd());
        StaticBuffer keyStart = query.getStart();
        StaticBuffer keyEnd = query.getEnd();
//        byte[] keyStartBytes = keyStart.as(StaticBuffer.ARRAY_FACTORY);
        byte[] keyStartBytes = keyStart.as(StaticBuffer.BB_FACTORY).array();
        //启动一个事务
        Transaction tx = getTransaction(txh);
        return new RecordIterator<KeyValueEntry>() {
            private RocksIterator iterator = tx.getIterator(new ReadOptions(), cfHandle);
//            private RocksIterator iterator = rocksdb.newIterator(cfHandle);
            private KeyValueEntry entry = null;
            private boolean isStart = true;

            //目的：seek一个在query范围内的值，则返回true，否则false
            @Override
            public boolean hasNext() {
                if(entry == null){
                    if(isStart) {
                        iterator.seek(keyStartBytes);
                        isStart = false;
                    }
                    byte[] keyBytes = iterator.key();
                    StaticBuffer key = new StaticArrayBuffer(keyBytes);
                    if(iterator.isValid() && key.compareTo(keyEnd)<0){
                        byte[] valueBytes = iterator.value();
                        StaticBuffer value = new StaticArrayBuffer(valueBytes);
                        entry = new KeyValueEntry(key, value);
                    }
                    try {
                        tx.commit();
                    } catch (RocksDBException e) {
                        e.printStackTrace();
                    }
                }
                return entry != null;
            }

            //目的：返回seek到的值
            @Override
            public KeyValueEntry next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                KeyValueEntry next = entry;
                entry = null;
                iterator.next();
                try {
                    tx.commit();
                } catch (RocksDBException e) {
                    e.printStackTrace();
                }
                return next;
            }

            @Override
            public void close() {
                iterator.close();
            }
        };
    }

    //待补充
    @Override
    public Map<KVQuery, RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    private static Transaction getTransaction(StoreTransaction txh) {
        Preconditions.checkArgument(txh!=null);
        return ((RocksdbTx) txh).getTransaction();
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, Integer ttl) throws BackendException {
//        log.info("beginning db={}, op=insert, tx={}", cfName, txh);
        Transaction tx = getTransaction(txh);
        try {

            byte[] keyBytes = key.as(StaticBuffer.ARRAY_FACTORY);
            byte[] valueBytes = value.as(StaticBuffer.ARRAY_FACTORY);
//            String encodingKey = getEncoding(keyBytes);
//            String encodingValue = getEncoding(valueBytes);
//            System.out.println(cfName + "*****" + encodingKey);
//            System.out.println(cfName + "#####" + encodingValue);

//            System.out.println(cfName + keyBytes.length);
//            String keyStr = key.toString();
//            String valueStr = value.toString();
            tx.put(cfHandle, keyBytes, valueBytes);
            tx.commit(); //一定不要忘了commit
//            log.info("If the key isn't exist, insert it, otherwise update it");
        } catch (RocksDBException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
//        log.info("beginning db={}, op=delete, tx={}", cfName, txh);
        Transaction tx = getTransaction(txh);
        try {
            byte[] keyBytes = key.as(StaticBuffer.ARRAY_FACTORY);
//            byte[] keyBytes = key.as(StaticBuffer.BB_FACTORY).array();
            tx.delete(keyBytes);
            tx.commit();
//            log.info("delete {}", new String(keyBytes));
        } catch (RocksDBException e) {
            throw new PermanentBackendException(e);
        }

    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
//        log.info("beginning db={}, op=get, tx={}", cfName, txh);

        Transaction tx = getTransaction(txh);
        try {
            byte[] keyBytes = key.as(StaticBuffer.ARRAY_FACTORY);
//            byte[] keyBytes = key.as(StaticBuffer.BB_FACTORY).array();
            final byte[] values = tx.get(cfHandle, new ReadOptions(), keyBytes);
            if(values == null){
                return null;
            }
            tx.commit();
//            log.info("find the value of {}", new String(keyBytes));
            return new StaticArrayBuffer(values);
        } catch (RocksDBException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return get(key, txh) != null;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        if (getTransaction(txh) == null) {
            log.warn("Attempt to acquire lock with transactions dsabled");
        } //else we need no locking
    }

    @Override
    public String getName() {
        return cfName;
    }

    @Override
    public void close() throws BackendException {
//        try {
//            cfHandle.close();
//        }catch (Exception e){
//            throw new PermanentBackendException(e);
//        }
    }

}
