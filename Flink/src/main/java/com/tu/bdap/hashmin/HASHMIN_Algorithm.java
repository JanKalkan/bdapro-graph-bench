package com.tu.bdap.hashmin;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.util.Collector;

public class HASHMIN_Algorithm {

	public static void main(String[] args) throws Exception {

		// Create execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// USA-Dataset
//		DataSet<Edge<Integer, Double>> edges = env.readTextFile(args[0]).filter(new FilterFunction<String>() {
//			@Override
//			public boolean filter(String value) throws Exception {
//				return value.startsWith("a");
//			}
//		}).flatMap(new FlatMapFunction<String, Edge<Integer, Double>>() {
//			@Override
//			public void flatMap(String value, Collector<Edge<Integer, Double>> out) throws Exception {
//				String[] values = value.split(" ");
//				out.collect(new Edge<Integer, Double>(Integer.parseInt(values[1]), Integer.parseInt(values[2]),
//						Double.parseDouble(values[3])));
//			}
//		});
		
		//Twitter Dataset
		DataSet<Edge<Integer, Double>> edges = env.readTextFile(args[0]).filter(new FilterFunction<String>() {
			@Override
			public boolean filter(String value) throws Exception {
				return Character.isDigit(value.charAt(0));
			}
		}).flatMap(new FlatMapFunction<String, Edge<Integer, Double>>() {
			@Override
			public void flatMap(String value, Collector<Edge<Integer, Double>> out) throws Exception {
				String[] values = value.split(" ");
				out.collect(new Edge<Integer, Double>(Integer.parseInt(values[0]), Integer.parseInt(values[1]),
						1.0));
			}
		});

		Graph<Integer, Double, Double> graph = Graph.fromDataSet(edges, new MapFunction<Integer, Double>() {
			@Override
			public Double map(Integer value) throws Exception {
				return (double) value;
			}
		}, env).getUndirected();

		Graph<Integer, Double, Double> result = graph.runVertexCentricIteration(new HashMinComputeFunction(), null, 20);

		// Extract the vertices as the result
		DataSet<Vertex<Integer, Double>> hashmin = result.getVertices();

		hashmin.collect();

	}

	public static final class HashMinComputeFunction extends ComputeFunction<Integer, Double, Double, Double> {

		@Override
		public void compute(Vertex<Integer, Double> vertex, MessageIterator<Double> messages) {

			if (getSuperstepNumber() == 1) {
				for (Edge<Integer, Double> e : getEdges()) {
					sendMessageTo(e.getTarget(), vertex.getValue());
				}
			} else {
				double minMessage = Double.POSITIVE_INFINITY;
				for (Double msg : messages) {
					minMessage = Math.min(minMessage, msg);
				}
				if (minMessage < vertex.getValue()) {
					setNewVertexValue(minMessage);
					for (Edge<Integer, Double> e : getEdges()) {
						sendMessageTo(e.getTarget(), minMessage);
					}
				}

			}
		}
	}

}
