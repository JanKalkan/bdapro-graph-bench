package com.tu.bdap.sv;

import com.tu.bdap.utils.GraphLoader;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

/**
 * Created by jan on 25.08.17.
 */
public class SVGraphLoader implements GraphLoader {

    @Override
    public Graph<Integer, Tuple2<Integer, Boolean>, NullValue> load(String path) {

        // Get execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Edge<Integer, NullValue>> edges = env.readTextFile(path)
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        System.out.println(value);
                        return value.startsWith("a");
                    }
                }).flatMap(new FlatMapFunction<String, Edge<Integer, NullValue>>() {
                    @Override
                    public void flatMap(String value, Collector<Edge<Integer, NullValue>> out) throws Exception {
                        String[] values = value.split(" ");
                        out.collect(new Edge<Integer, NullValue>(Integer.parseInt(values[1]), Integer.parseInt(values[2]),
                                new NullValue()));
                    }
                });

        //Create graph and initialize the vertex value
        Graph<Integer, Tuple2<Integer, Boolean>, NullValue> graph = Graph.fromDataSet(edges, new MapFunction<Integer, Tuple2<Integer, Boolean>>() {
            @Override
            public Tuple2<Integer, Boolean> map(Integer vertexKey) throws Exception {
                return new Tuple2<Integer, Boolean>(vertexKey, null);
            }
        }, env).getUndirected();

        return graph;
    }

}
