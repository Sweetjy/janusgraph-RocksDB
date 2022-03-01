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

import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * @author Sweetjy
 * @date 2021/11/24
 */
//在properties配置文件中进行设置，storage.rocksdb.xxx，其中xxx是configOption的name
public interface RocksDBConfig {

    ConfigNamespace ROCKS = new ConfigNamespace(
        GraphDatabaseConfiguration.STORAGE_NS,
        "rocksdb",
        "rocksdb configuration"
    );

    ConfigOption<Boolean> ROCKSDB_SMALLDB = new ConfigOption<>(
        ROCKS,
        "optimizeForSmallDb",
        "Use this if your DB is very small (like under 1GB) and you don't want to spend lots of memory for memtables.",
        ConfigOption.Type.LOCAL,
        Boolean.class,
        true
    );

    ConfigOption<Boolean> ROCKSDB_COMPACTION = new ConfigOption<>(
        ROCKS,
        "disable-compaction",
        "Disable automatic compactions",
        ConfigOption.Type.LOCAL,
        Boolean.class,
        false
    );

    ConfigOption<Integer> TARGET_FILE_SIZE_BASE = new ConfigOption<>(
        ROCKS,
        "target_file_size_base",
        "target_file_size_base the target size of a level-0 file. For example, if target_file_size_base is 2MB and" +
            "target_file_size_multiplier is 10, then each file on level-1 will be 2MB, and each file on level 2 will be 20MB,",
        ConfigOption.Type.LOCAL,
        Integer.class,
        4 * 1048576
    );

    ConfigOption<Integer> ROCKSDB_WRITE_BUFFER = new ConfigOption<>(
        ROCKS,
        "WriteBufferSize",
        "This is the size of an individual write buffer size.",
        ConfigOption.Type.LOCAL,
        Integer.class,
        16 * 1048576
    );

    ConfigOption<Integer> BLOOM_FILTER = new ConfigOption<>(
        ROCKS,
        "Bits_per_key",
        "This is the size of per key for bloom filter",
        ConfigOption.Type.LOCAL,
        Integer.class,
        8
    );

    ConfigOption<Integer> LEVEL0_NUM_COMPACTION_TRIGGER = new ConfigOption<>(
        ROCKS,
        "level0_file_num_compaction_trigger",
        "Number of files to trigger level-0 compaction.",
        ConfigOption.Type.LOCAL,
        Integer.class,
        4
    );

    ConfigOption<Integer> MAX_BYTES_FOR_LEVEL_BASE = new ConfigOption<>(
        ROCKS,
        "max_bytes_for_level_base",
        "Control maximum total data size for a level.",
        ConfigOption.Type.LOCAL,
        Integer.class,
        16 * 1048576
    );

}
