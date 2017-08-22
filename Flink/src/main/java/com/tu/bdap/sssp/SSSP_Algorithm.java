package com.tu.bdap.sssp;

import java.awt.print.Printable;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.util.Collector;

public class SSSP_Algorithm {

	public static void main(String[] args) throws Exception {

		// Create execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Integer, Double>> edges = env.readTextFile("/home/johannes/Downloads/test.txt")
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
				return (double) value;
			}
		}, env).getUndirected();
		
		final long srcVertexId = 1;

		Graph<Integer, Double, Double> result = graph.runVertexCentricIteration(new SSSPComputeFunction(srcVertexId),
				new SSSPCombiner(), 20);

		// Extract the vertices as the result
		DataSet<Vertex<Integer, Double>> singleSourceShortestPaths = result.getVertices();

		singleSourceShortestPaths.print();

	}

	@SuppressWarnings("serial")
	public static final class SSSPComputeFunction extends ComputeFunction<Integer, Double, Double, Double> {

		private final long srcId;

		public SSSPComputeFunction(long src) {
			this.srcId = src;
		}

		public void compute(Vertex<Integer, Double> vertex, MessageIterator<Double> messages) {

			double minDistance = (vertex.getId().equals(srcId)) ? 0d : Double.POSITIVE_INFINITY;

			for (Double msg : messages) {
				minDistance = Math.min(minDistance, msg);
			}

			if (minDistance < vertex.getValue()) {
				setNewVertexValue(minDistance);
				for (Edge<Integer, Double> e : getEdges()) {
					sendMessageTo(e.getTarget(), minDistance + e.getValue());
				}
			}
		}
	}

	/**
	 * The messages combiner. Out of all messages destined to a target vertex,
	 * only the minimum distance is propagated.
	 */
	@SuppressWarnings("serial")
	public static final class SSSPCombiner extends MessageCombiner<Integer, Double> {

		public void combineMessages(MessageIterator<Double> messages) {

			double minMessage = Double.POSITIVE_INFINITY;
			for (Double msg : messages) {
				minMessage = Math.min(minMessage, msg);
			}
			sendCombinedMessage(minMessage);
		}
	}

}