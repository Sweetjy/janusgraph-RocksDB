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

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;

/**
 * @author Sweetjy
 * @date 2021/11/26
 */
public class RocksdbTx extends AbstractStoreTransaction {
    private volatile Transaction tx;

    public RocksdbTx(BaseTransactionConfig config, Transaction tx) {
        super(config);
        this.tx = tx;
    }

    public Transaction getTransaction() {
        return tx;
    }

    @Override
    public synchronized void commit() throws BackendException {
        super.commit();
        try {
            tx.commit();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void rollback() throws BackendException {
        super.rollback();
        try {
            tx.rollback();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
