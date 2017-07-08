package com.tu.bdap.pagerank;

/**
 * Created by simon on 08.07.17..
 */
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.graph.*;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.NullValue;

public class PageRank {


/**
 * Created by simon on 01.06.17..
 */


    private static final double DAMPENING_FACTOR = 0.85;

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Long, Long>> vertexTuples = env.readTextFile(args[0])
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return s.startsWith("a");
                    }
                })
                .map(new MapFunction<String, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long,Long> map(String value) throws Exception {
                String[] split=value.split(" ");
                Tuple2<Long, Long> tuple= new Tuple2<>(Long.parseLong(split[1]),Long.parseLong(split[2]));
                return tuple;
            }

        });


        Graph<Long, NullValue, Double> graph = Graph.fromTuple2DataSet(vertexTuples, env)
                .mapEdges(new MapFunction<Edge<Long,NullValue>, Double>() {
                    @Override
                    public Double map(Edge<Long, NullValue> edge) throws Exception {
                        return 1.0;
                    }
                });

        Graph<Long, Double, Double> pr = graph.mapVertices(new MapFunction<Vertex<Long, NullValue>, Double>() {
            @Override
            public Double map(Vertex<Long, NullValue> integerNullValueVertex) throws Exception {
                return 1.0;
            }
        });

        //DataSet<Tuple2<Long, Double>> Weights = pr.reduceOnEdges(new Gamma(), EdgeDirection.OUT);

        pr = pr.runVertexCentricIteration(new ComputePR(), new CombinePR(), 20);

        pr.getVertices().print();
       // Graph<Long, Tuple2<Double,Double>, Double> PR = pr
    }
    static final class Gamma implements ReduceEdgesFunction<Double> {
        @Override
        public Double reduceEdges(Double firstEdgeValue, Double secondEdgeValue) {
            double sum = firstEdgeValue+secondEdgeValue;
            return sum;
        }


    }

    private static class ComputePR extends ComputeFunction<Long, Double, Double, Double> {

        @Override
        public void compute(Vertex<Long, Double> vertex, MessageIterator<Double> messages) throws Exception {


            double sum = 0.0;
            for (double msg : messages) {
                sum += msg;
            }

            int gamma = 0;
            for (Edge<Long, Double> e : getEdges()) {
                gamma++;
            }
            double val = 0.15+0.85*sum;

            if (getSuperstepNumber()==0){
                for (Edge<Long, Double> e : getEdges()) {
                    sendMessageTo(e.getTarget(), 1.0);
                }
            }
            else {
                setNewVertexValue(val);

                for (Edge<Long, Double> e : getEdges()) {
                    sendMessageTo(e.getTarget(), vertex.getValue()/gamma);
                }

            }


        }
    }
    public static final class CombinePR  extends MessageCombiner<Long, Double> {

        public void combineMessages(MessageIterator<Double> messages) {

            double sum = 0.0;
            for (double msg : messages) {
                sum += msg;
            }
            sendCombinedMessage(sum);
        }
    }
}