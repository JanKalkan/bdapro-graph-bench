package com.tu.bdap.sssp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.SingleSourceShortestPaths;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class SSSP_Algorithm {
	public static void main(String[] args) throws Exception {

		// Create execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Read DataSet
		DataSet<Edge<Long, Double>> edges = env.readTextFile(args[0])
				.flatMap(new FlatMapFunction<String, Edge<Long, Double>>() {

					@Override
					public void flatMap(String s, Collector<Edge<Long, Double>> arg1) throws Exception {
						if (s.startsWith("a")) {
							arg1.collect(new Edge<Long, Double>(Long.parseLong(s.split(" ")[1]),
									Long.parseLong(s.split(" ")[2]), Double.parseDouble(s.split(" ")[3])));
						}

					}
				});

		// Create Graph from Dataset
		Graph<Long, NullValue, Double> graph = Graph.fromDataSet(edges, env);

		// Run SSSP and print the result (Integer.MAX_VALUE = numIterations)
		graph.run(new SingleSourceShortestPaths<Long, NullValue>(1L, Integer.MAX_VALUE)).print();

	}

}
