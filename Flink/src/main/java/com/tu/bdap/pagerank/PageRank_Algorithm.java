package com.tu.bdap.pagerank;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.link_analysis.PageRank;
import org.apache.flink.types.NullValue;

public class PageRank_Algorithm {
	
	public static void main(String[] args) throws Exception {

        final double DAMPING_FACTOR = 0.15;
        final int ITERATIONS = 100;

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, String>> edges = env.readTextFile(args[0]).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("a");
            }
        }).map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] values = value.split(" ");
                return new Tuple2<String, String>(values[1], values[2]);
            }
        });

        Graph<String, NullValue,NullValue> graph = Graph.fromTuple2DataSet(edges, env);

        PageRank pageRank = new PageRank(DAMPING_FACTOR,ITERATIONS);


        DataSet<PageRank.Result<String>> result = pageRank.runInternal(graph);

        result.print();

    }

}
