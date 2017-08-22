package com.tu.bdap.coloring;

import com.tu.bdap.pagerank.PageRank;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.*;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.NullValue;

import java.util.Random;

import static java.lang.Math.pow;

/**
 * Created by simon on 12.08.17..
 */
public class GC_Algorithm {
    public static void main(String[] args) throws Exception{

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Long,Long>> edges = env.readTextFile(args[0])
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return s.startsWith("a");
                    }
                })
                .map(new MapFunction<String, Tuple2<Long,Long>>(){
                    @Override
                    public Tuple2<Long, Long> map(String value) throws Exception {
                        String[] s =value.split(" ");
                        return new Tuple2<>(Long.parseLong(s[1]),Long.parseLong(s[2]));
                    }
                });


        Graph<Long, Double, Double> graph = Graph.fromTuple2DataSet(edges,env)
                .mapEdges(new MapFunction<Edge<Long,NullValue>, Double>() {
                    @Override
                    public Double map(Edge<Long, NullValue> edge) throws Exception {
                        return 1.0;
                    }

                }).mapVertices(new MapFunction<Vertex<Long, NullValue>, Double>() {
                    @Override
                    public Double map(Vertex<Long, NullValue> vertex) throws Exception {
                        return 0.0;
                    }
                });
        graph = graph.getUndirected();

        DataSet<Tuple2<Long, Double>> neigbours = graph.reduceOnEdges(new Gamma(), EdgeDirection.OUT);


        DataSet<Vertex<Long, Tuple3<Integer, Integer, Double>>> vertices = graph.joinWithVertices(neigbours, new VertexJoinFunction<Double, Double>() {
            @Override
            public Double vertexJoin(Double a, Double b) throws Exception {
                return b;
            }
        }).getVertices().map(new MapFunction<Vertex<Long, Double>, Vertex<Long, Tuple3<Integer, Integer, Double>>>() {
            @Override
            public Vertex<Long, Tuple3<Integer, Integer, Double>> map(Vertex<Long, Double> vertex) throws Exception {
                // Values: Colour,Status, Neighbours
                return new Vertex<>(vertex.f0, new Tuple3<>(-1,-1,vertex.f1));
            }
        });

        Graph<Long, Tuple3<Integer, Integer, Double>, Double> gc = Graph.fromDataSet(vertices, graph.getEdges(), env);

        Graph<Long, Tuple3<Integer, Integer, Double>, Double> colour = gc.runVertexCentricIteration(new ComputeGC(), new CombineGC(), 50);

        colour.getVertices().print();

    }
    static final class Gamma implements ReduceEdgesFunction<Double> {
        @Override
        public Double reduceEdges(Double firstEdgeValue, Double secondEdgeValue) {
            double sum = firstEdgeValue+secondEdgeValue;
            return sum;
        }


    }
    public static final class ComputeGC extends ComputeFunction<Long,  Tuple3<Integer, Integer, Double>, Double, Tuple2<Long, Integer>> {

        public void compute(Vertex<Long, Tuple3<Integer, Integer, Double>> vertex, MessageIterator<Tuple2<Long, Integer>> messages) {

            Long min = Long.MAX_VALUE;
            int removedNeighbours = 0;
            Double neighbours = vertex.getValue().f2;
            for (Tuple2<Long, Integer> msg : messages) {
                if (msg.f0 < min) {
                    min = msg.f0;
                }
                if (msg.f1 == 1) {
                    removedNeighbours += msg.f1;
                }
            }
            //resolve conflict
            neighbours = vertex.getValue().f2 - removedNeighbours;

            if (vertex.getValue().f1 == 0) {
                if (min > vertex.getId()) {
                    int colour = vertex.getValue().f0;
                    setNewVertexValue(new Tuple3<>(colour, 1, neighbours));
                    for (Edge<Long, Double> e : getEdges()) {
                        sendMessageTo(e.getTarget(), new Tuple2<>(vertex.getId(), 1));
                    }
                    return;
                }
            }
            if (vertex.getValue().f1 == 1) {
                setNewVertexValue(new Tuple3<>(vertex.getValue().f0, 1, neighbours));
                return;
            }

            // select tentative node
            Random r = new Random();
            int colour = getSuperstepNumber();
            int status = -1;

            if ( r.nextDouble() <= 1.0/(2*neighbours) || neighbours==0) {

                status = 0;
                setNewVertexValue(new Tuple3<>(colour, status, neighbours));
                for (Edge<Long, Double> e : getEdges()) {
                    sendMessageTo(e.getTarget(), new Tuple2<>(vertex.getId(), 0));
                }
                return;
            }
            for (Edge<Long, Double> e : getEdges()) {
                sendMessageTo(e.getTarget(), new Tuple2<>(Long.MAX_VALUE, 0));
            }
        }
    }
    public static final class CombineGC  extends MessageCombiner<Long,  Tuple2<Long, Integer>> {

        public void combineMessages(MessageIterator< Tuple2<Long, Integer>> messages) {

            Long min = Long.MAX_VALUE;
            int removedNeighbours = 0;
            for (Tuple2<Long, Integer> msg : messages) {
                if(msg.f1 == 1){
                    removedNeighbours+= 1;
                }
                else {
                    if (msg.f0 < min){
                        min = msg.f0;
                    }
                }
            }
            sendCombinedMessage(new Tuple2<Long, Integer>(min,removedNeighbours));

        }
    }
}

