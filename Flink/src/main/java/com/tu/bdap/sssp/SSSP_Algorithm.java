package com.tu.bdap.sssp;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.SingleSourceShortestPaths;
import org.apache.flink.util.Collector;

public class SSSP_Algorithm {

	public static void main(String[] args) throws Exception {

		// Create execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Integer, Double>> edges = env.readTextFile("/home/simon/ny.txt")
				.filter(new FilterFunction<String>() {
					@Override
					public boolean filter(String value) throws Exception {
						return value.startsWith("a");
					}
				}).flatMap(new FlatMapFunction<String, Edge<Integer, Double>>() {
					@Override
					public void flatMap(String value, Collector<Edge<Integer, Double>> out) throws Exception {
						String[] values = value.split(" ");
						out.collect(new Edge<Integer, Double>(Integer.parseInt(values[1]), Integer.parseInt(values[2]),
								Double.parseDouble(values[3])));
					}
				});

		Graph<Integer, Double, Double> graph = Graph.fromDataSet(edges, new MapFunction<Integer, Double>() {
			@Override
			public Double map(Integer value) throws Exception {
				return 0.0;
			}
		}, env);

		Object result =graph.run(new SingleSourceShortestPaths(1, 20));
		System.out.print(result);
	}

}