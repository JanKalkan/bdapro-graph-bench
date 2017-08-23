package com.tu.bdap.bmm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.PageRank;
import org.apache.flink.util.Collector;

public class BMM_Algorithm {

	public static void main(String[] args) throws Exception {

		// Create execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Integer, Double>> edges = env.readTextFile("C:/Users/Johannes/Desktop/test.txt")
				.filter(new FilterFunction<String>() {
					@Override
					public boolean filter(String value) throws Exception {
						return Character.isDigit(value.charAt(0));
					}
				}).flatMap(new FlatMapFunction<String, Edge<Integer, Double>>() {
					@Override
					public void flatMap(String value, Collector<Edge<Integer, Double>> out) throws Exception {
						String[] values = value.split(" ");
						out.collect(new Edge<Integer, Double>(Integer.parseInt(values[0]), Integer.parseInt(values[1]),
								0.0));
					}
				});
		

		Graph<Integer, String, Double> graph = Graph.fromDataSet(edges, new MapFunction<Integer, String>() {
			@Override
			public String map(Integer value) throws Exception {
				return "R";
			}
		}, env);
		


		graph.getVertices().print();

	}

}