package org.janusgraph.mytest.csvload;
//package com.cl.janus;

import org.janusgraph.core.*;
import org.janusgraph.graphdb.database.management.ManagementSystem;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.janusgraph.mytest.base.BaseTest.initGraph;

/**
 * @author Sweetjy
 * @date 2021/12/30
 */
class LoadVertexAndEdge {

    private final Map<Integer, Long> vertexes = new ConcurrentHashMap<>();

    private int VERTEX_NUM = 10;
    private int ratio = 2; //平均一个点有几条边

    private int commitBatch = 5000;

    LoadVertexAndEdge(int commitBatch, String num, String ratio){
        this.commitBatch = commitBatch;
        this.ratio = Integer.parseInt(ratio);
        VERTEX_NUM = Integer.parseInt(num);
    }

    /**
     * 添加点
     * @param JanusG
     */
    public synchronized void addVertex(JanusGraph JanusG) {
        JanusGraphTransaction tx = JanusG.newTransaction();
        System.out.println("loading vertex");
        for (int nodeid = 0; nodeid<VERTEX_NUM; nodeid++){
            JanusGraphVertex srcVertex = tx.addVertex("MyNode");
            srcVertex.property("node", String.valueOf(nodeid));
//            System.out.printf("node = %d, id = %d\n", nodeid, srcVertex.longId());
            vertexes.put(nodeid, (Long) srcVertex.id());   //保存顶点的信息

            if (nodeid % commitBatch == 0) {
                tx.commit();
                tx = JanusG.newTransaction();
            }
        }
        tx.commit();
        notifyAll();
    }


    /**
     * 添加边
     * @param JanusG
     */
    public synchronized void addEdges(JanusGraph JanusG) {
        try {
            wait();

            JanusGraphTransaction tx = JanusG.newTransaction();
            System.out.println("loading edges");
            for (int nodeid = 0; nodeid < VERTEX_NUM; nodeid++) {
                ArrayList<Integer> records = new ArrayList<>();   //记录生成的随机数，保证生成不重复的随机数
                records.add(nodeid);
                Random rd = new Random(nodeid);
                for (int i = 0; i < ratio; i++) {
                    JanusGraphVertex srcVertex = tx.getVertex(vertexes.get(nodeid));
                    int tgt = rd.nextInt(VERTEX_NUM);
                    while (records.contains(tgt))
                        tgt = rd.nextInt(VERTEX_NUM);
                    records.add(tgt);
                    JanusGraphVertex dstVertex = tx.getVertex(vertexes.get(tgt));
                    JanusGraphEdge edge = srcVertex.addEdge("MyEdge", dstVertex);

//                    System.out.println(nodeid + "->" + tgt);
                }
                tx.commit();
                tx = JanusG.newTransaction();
            }
            tx.commit();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}

public class Loader{

    private static final int THREAD_NUM = 4;
    private static ExecutorService pool = Executors.newFixedThreadPool(THREAD_NUM, new MyThreadFactory());

    private static int commitBatch = 5000;

    public static void main(String args[]){
        String properties = "E:\\IntelliJ IDEA\\workspace\\janusgraph\\janusgraph-dist\\src\\assembly\\cfilter\\conf\\" +
            "janusgraph-rocksdb-lucene.properties";  //berkeleyje
        String num = "1000";
        String ratio = "3";
        JanusGraph JanusG = JanusGraphFactory.open(properties);

//        创建图结构
        ManagementSystem mgmt = (ManagementSystem) JanusG.openManagement();
        mgmt.makeEdgeLabel("MyEdge").make();
        mgmt.makeVertexLabel("MyNode").make();
        PropertyKey id_key = mgmt.makePropertyKey("node").dataType(String.class).make();
        mgmt.buildIndex("byId", JanusGraphVertex.class).addKey(id_key).unique().buildCompositeIndex();
        mgmt.commit();

        LoadVertexAndEdge load = new LoadVertexAndEdge(commitBatch, num, ratio);

        System.out.println("begin timing： ");
        long startTime=System.nanoTime();
        load.addVertex(JanusG);
        load.addEdges(JanusG);
//        pool.execute(()->load.addEdges(JanusG));
//        pool.execute(()->load.addVertex(JanusG));
        long endTime=System.nanoTime();


        pool.shutdown();
        try {
            pool.awaitTermination(1440, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("程序运行时间： "+((endTime-startTime) / 1000 / 1000 / 1000.0)+"s");

        JanusG.close();
    }


    /** this class define name for threads
     */
    public static class MyThreadFactory implements ThreadFactory {
        private int counter = 0;
        public Thread newThread(Runnable r) {
            return new Thread(r, "" + counter++);
        }
    }
}