// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.mytest;

import jdk.nashorn.internal.ir.annotations.Ignore;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.janusgraph.mytest.base.BaseTest;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.janusgraph.core.attribute.Geo.geoWithin;

/**
 * 图库-第一个单元测试
 * @author sweetjy
 * @date 2021/11/17
 */
@Ignore
public class JanusGraphFirstTest extends BaseTest {

    @Test
    public void dropGraphTest() throws BackendException {
        //删除原来的图
        JanusGraphFactory.drop(graph);
    }
    @Test
    public void loadGraphTest(){
        // 使用GraphOfTheGodsFactory加载“The Graph of the Gods”图，这是JanusGraph用于测试自定义的一个图
        // 导入一个其他的图则使用GraphFactory类
        //图只需要load一次，就会将图加载到
        GraphOfTheGodsFactory.load(graph); // 第一次运行时添加，之后的运行将该语句注释掉，不需要重复的load
    }

    @Test
    public void firstTest() {
        loadGraphTest();
        // 获取图遍历对象实例
        initGraph();
        g = graph.traversal();


        //遍历图的所有顶点
        System.out.println("图的所有顶点如下");
        //输出图的节点和属性
        List<Vertex> vertices = g.V().toList();
        //获取每一个节点的属性
        for(Vertex each : vertices){
            String label = each.label();
            GraphTraversal<Vertex, Map<Object, Object>> vertexMapGraphTraversal = g.V(each).valueMap();
            List<Map<Object, Object>> Maps = vertexMapGraphTraversal.toList();
            System.out.printf("%s=====", each);
            for (Map<Object, Object> info : Maps) {
                info.forEach((key,value) -> System.out.printf(label + ", " + key + ":" + value));
            }
            System.out.println();
        }
//        GraphTraversal<Vertex, Map<Object, Object>> allVertexInfo = g.V().elementMap();
//        List<Map<Object, Object>> allVertexInfoList = allVertexInfo.toList();
//        for (Map<Object, Object> proMap : allVertexInfoList) {
//            System.out.printf("[");
//            proMap.forEach((key,value) -> System.out.printf(key + ":" + value+","));
//            System.out.printf("]\n");
//        }

        //遍历图的所有边
        System.out.println("图的所有边如下");
        List<Edge> edges = g.E().toList();
        for (Edge each : edges) {
            String label = each.label();
            GraphTraversal<Edge, Map<Object, Object>> edgeMapGraphTraversal = g.E(each).valueMap();
            List<Map<Object, Object>> Maps = edgeMapGraphTraversal.toList();
            System.out.printf("%s--->%s, %s", each.outVertex(),each.inVertex(),each.label());
            for (Map<Object, Object> info : Maps) {
                info.forEach((key,value) -> System.out.printf("," +key + ":" + value));
            }
            System.out.println();
        }


        // 获取属性"name"为"saturn"的节点
        System.out.println("获取某一个节点的属性值");
        Vertex saturn = g.V().has("name", "saturn").next();
        // 获取上述节点对应的所有属性的kv
        GraphTraversal<Vertex, Map<Object, Object>> vertexMapGraphTraversal = g.V(saturn).valueMap();
        List<Map<Object, Object>> saturnProMaps = vertexMapGraphTraversal.toList();
        for (Map<Object, Object> proMap : saturnProMaps) {
            proMap.forEach((key,value) -> System.out.println("saturn" + key + ":" + value+","));
        }

        // 获取上述节点的father的father的姓名，也就是grandfather的姓名
        System.out.println("获取与某一个节点相关的另一个节点的属性值");
//        GraphTraversal<Vertex, Vertex> vertex = g.V().has("name", "hercules").repeat(__.out().dedup()).times(1).values("name");
//        System.out.println(vertex.next());
        GraphTraversal<Vertex, Object> values = g.V(saturn).in("father").in("father").values("name");
        String name = String.valueOf(values.next());
        System.out.println("saturn's " + "grandfather name is:" + name);

        // 获取在(latitude:37.97 and long:23.72)50km内的所有节点
        System.out.println("获取满足条件的边（边的属性place满足在某个范围内）的顶点");
        GraphTraversal<Edge, Edge> place = g.E().has("place", geoWithin(Geoshape.circle(37.97, 23.72, 50)));
        GraphTraversal<Edge, Map<String, Object>> node = place.as("source")
            .inV().as("to")
            .select("source")
            .outV().as("from")
            .select("from", "to").by("name");
        List<Map<String, Object>> maps = node.toList();
        for (Map<String, Object> map : maps) {
            map.forEach((key,value) -> System.out.printf(key + ":" + value + ","));
            System.out.println();
        }
    }

    @Test
    public void secondTest(){
        initGraph();
        g = graph.traversal();

//        //输出图的节点和属性
        List<Vertex> vertices = g.V().toList();
        System.out.println("图的顶点个数为"+vertices.size());
        String update = UUID.randomUUID().toString().replaceAll("-", "");
        GraphTraversal<Vertex, Vertex> size = g.V().has("node", 2)
            .property("node",update);
        System.out.println(size.next().property("node"));
//        //获取每一个节点的属性
//        for(Vertex each : vertices){
//            GraphTraversal<Vertex, Map<Object, Object>> vertexMapGraphTraversal = g.V(each).valueMap();
//            List<Map<Object, Object>> Maps = vertexMapGraphTraversal.toList();
//            System.out.printf("%s=====", each);
//            for (Map<Object, Object> info : Maps) {
//                info.forEach((key,value) -> System.out.println(key + ":" + value));
//            }
//        }
//        List<Edge> edges = g.E().toList();
//        System.out.println("图的边个数为"+edges.size());
//        for (Edge each : edges) {
//            GraphTraversal<Edge, Map<Object, Object>> edgeMapGraphTraversal = g.E(each).valueMap();
//            List<Map<Object, Object>> Maps = edgeMapGraphTraversal.toList();
//            System.out.printf("%s--->%s, %s", each.outVertex(),each.inVertex(),each.label());
//            for (Map<Object, Object> info : Maps) {
//                info.forEach((key,value) -> System.out.printf("," +key + ":" + value));
//            }
//        }
//        Vertex v = g.V().has("node", "1").next();
//        int size = g.V(v).repeat(__.in().dedup()).times(1).emit().toSet().size();
//        System.out.println(size);
    }
    @Test
    public void ManageTest() {
        //可以查看元数据
        JanusGraphManagement mgmt = graph.openManagement();
        String schema = mgmt.printSchema();
        String index = mgmt.printIndexes();
        String keys = mgmt.printPropertyKeys();
        String edges = mgmt.printEdgeLabels();
        String vertexs = mgmt.printVertexLabels();
    }
}
