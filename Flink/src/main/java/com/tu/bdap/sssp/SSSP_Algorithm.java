package com.tu.bdap.sssp;

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

		DataSet<Edge<Long, Double>> edges = env.readTextFile(args[0])
				.filter(new FilterFunction<String>() {
					@Override
					public boolean filter(String value) throws Exception {
						return value.startsWith("a");
					}
				}).flatMap(new FlatMapFunction<String, Edge<Long, Double>>() {
					@Override
					public void flatMap(String value, Collector<Edge<Long, Double>> out) throws Exception {
						String[] values = value.split(" ");
						out.collect(new Edge<Long, Double>(Long.parseLong(values[1]), Long.parseLong(values[2]),
								Double.parseDouble(values[3])));
					}
				});

		Graph<Long, Double, Double> graph = Graph.fromDataSet(edges, new MapFunction<Long, Double>() {
			@Override
			public Double map(Long value) throws Exception {
				return Double.POSITIVE_INFINITY;
			}
		}, env);

		final long srcVertexId = 1;

		Graph<Long, Double, Double> result = graph.runVertexCentricIteration(new SSSPComputeFunction(srcVertexId),
				new SSSPCombiner(), 20);
		// Extract the vertices as the result
		DataSet<Vertex<Long, Double>> singleSourceShortestPaths = result.getVertices();

		singleSourceShortestPaths.collect();
		

	}

	@SuppressWarnings("serial")
	public static final class SSSPComputeFunction extends ComputeFunction<Long, Double, Double, Double> {

		private final long srcId;

		public SSSPComputeFunction(long src) {
			this.srcId = src;
		}

		@Override
		public void compute(Vertex<Long, Double> vertex, MessageIterator<Double> messages) {

			double minDistance = (vertex.getId().equals(srcId)) ? 0d : Double.POSITIVE_INFINITY;

			for (Double msg : messages) {
				minDistance = Math.min(minDistance, msg);
			}

			if (minDistance < vertex.getValue()) {
				setNewVertexValue(minDistance);
				for (Edge<Long, Double> e : getEdges()) {
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
	public static final class SSSPCombiner extends MessageCombiner<Long, Double> {

		@Override
		public void combineMessages(MessageIterator<Double> messages) {

			double minMessage = Double.POSITIVE_INFINITY;
			for (Double msg : messages) {
				minMessage = Math.min(minMessage, msg);
			}
			sendCombinedMessage(minMessage);
		}
	}

}