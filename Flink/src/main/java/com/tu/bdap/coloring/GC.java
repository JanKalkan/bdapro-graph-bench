package com.tu.bdap.coloring;

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
public class GC {
    public static void main(String[] args) throws Exception{

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Long,Long>> edges = env.readTextFile(args[0])
        		.filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return Character.isDigit(s.charAt(0));
                    }
                })
                .map(new MapFunction<String, Tuple2<Long,Long>>(){
                    @Override
                    public Tuple2<Long, Long> map(String value) throws Exception {
                        String[] s =value.split(" ");
                        return new Tuple2<>(Long.parseLong(s[0]),Long.parseLong(s[1]));
                    }
                });


        Graph graph = Graph.fromTuple2DataSet(edges,env).mapVertices(new MapFunction<Vertex<Long,NullValue>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Vertex<Long, NullValue> longNullValueVertex) throws Exception {
                return new Tuple2<>(-1,-1);
            }
        });

        Graph gc = graph.getUndirected();






        Graph<Long, Tuple2<Integer, Integer>, Double> colour = gc.runVertexCentricIteration(new ComputeGC(), new CombineGC(), 10);

        colour.getVertices().print();

    }
    static final class Gamma implements ReduceEdgesFunction<Double> {
        @Override
        public Double reduceEdges(Double firstEdgeValue, Double secondEdgeValue) {
            double sum = firstEdgeValue+secondEdgeValue;
            return sum;
        }
    }
    
    public static final class ComputeGC extends ComputeFunction<Long,  Tuple2<Integer, Integer>, Double, Tuple2<Long, Integer>> {

        public void compute(Vertex<Long, Tuple2<Integer, Integer>> vertex, MessageIterator<Tuple2<Long, Integer>> messages) {

        	
            if (vertex.getValue().f1 == 1) {
                return;
            }
            Long min = Long.MAX_VALUE;
            int aliveNeighbours = 0;
            for (Tuple2<Long, Integer> msg : messages) {
                if (msg.f0 < min) {
                    min = msg.f0;
                }
                if (msg.f1 == 1) {
                	aliveNeighbours += msg.f1;
                }
            }
            
            // select tentative node
            Random r = new Random();
            int colour = getSuperstepNumber();
            int status = -1;
            if(getSuperstepNumber()==1){
                sendMessageToAllNeighbors(new Tuple2<>(Long.MAX_VALUE, 1));
                return;
            }
            int neighbours = aliveNeighbours;
            
            
            if(neighbours==0) {
            	status = 1;
                setNewVertexValue(new Tuple2<>(colour, status));
                return;
             }
            
            // resolve conflict
            if (vertex.getValue().f1 == 0 & getSuperstepNumber()>2) {
                if (min > vertex.getId()) {
                    colour = vertex.getValue().f0;
                    setNewVertexValue(new Tuple2<>(colour, 1));
                    sendMessageToAllNeighbors(new Tuple2<>(Long.MAX_VALUE, 0));
                    return;
                }
                else {
                	setNewVertexValue(new Tuple2<>(colour, 0));
                    sendMessageToAllNeighbors(new Tuple2<>(vertex.getId(), 1));
                    return;
                }
            }

            
            if ( r.nextDouble() <= 1.0/(2.*neighbours)) {
                setNewVertexValue(new Tuple2<>(colour, 0));
                sendMessageToAllNeighbors(new Tuple2<>(vertex.getId(), 1));
                return;
                
            }
            setNewVertexValue(new Tuple2<>(-1, -1));
            sendMessageToAllNeighbors(new Tuple2<>(Long.MAX_VALUE, 1));

            
        }
    }
    public static final class CombineGC  extends MessageCombiner<Long,  Tuple2<Long, Integer>> {

        public void combineMessages(MessageIterator< Tuple2<Long, Integer>> messages) {

            Long min = Long.MAX_VALUE;
            int neighbours = 0;
            for (Tuple2<Long, Integer> msg : messages) {
            	neighbours+=msg.f1;
                if (msg.f0 < min){
                    min = msg.f0;
                }
            
            }
            sendCombinedMessage(new Tuple2<Long, Integer>(min,neighbours));
        }
    }
}
