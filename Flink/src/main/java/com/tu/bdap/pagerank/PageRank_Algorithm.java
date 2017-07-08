package com.tu.bdap.pagerank;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.PageRank;
import org.apache.flink.util.Collector;

public class PageRank_Algorithm {
	
	public static void main(String[] args) throws Exception {

		// Create execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Integer, Double>> edges = env.readTextFile("/home/johannes/Dropbox/BDAPRO_Share/Datasets/ny.txt").filter(new FilterFunction<String>() {
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
				return 1.0;
			}
		}, env);
		
		//graph.run(new SingleSourceShortestPaths<Integer>(1, 20)).print();;
		PageRank<Integer> pr = new PageRank<>(0.01, 20);
		pr.run(graph).print();
    }

}