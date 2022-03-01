package org.janusgraph.mytest.base;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * @author sweetjy
 * @date 2021/11/17
 */
public class BaseTest {
    public static JanusGraph graph = null;
    public static GraphTraversalSource g = null;
    private static String properties = "E:\\IntelliJ IDEA\\workspace\\janusgraph\\janusgraph-dist\\src\\assembly\\cfilter\\conf\\" +
        "janusgraph-rocksdb-lucene.properties";
//    private static String properties = "/home/ljy/janusgraph-benchmark/conf/janusgraph-rocksdb-lucene.properties";

    @BeforeClass
    public static void initGraph(){
        graph = JanusGraphFactory
            .open(properties);
        //        g = graph.traversal();berkeleyje
    }

    @AfterClass
    public static void closeGraph() throws Exception {
        if (g != null){
            // 提交失败，会抛出throw new JanusGraphException("Could not commit transaction due to exception during persistence", e);
            g.tx().commit();
            g.close();
        }
        if (graph != null){
            graph.tx().commit();
            graph.close();
        }
    }
}
