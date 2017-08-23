package com.tu.bdap.hashmin;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSASingleSourceShortestPaths;
import org.apache.flink.graph.library.SingleSourceShortestPaths;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.util.Collector;

import com.tu.bdap.sssp.SSSP_Algorithm.SSSPComputeFunction;

public class HASHMIN_Algorithm {

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
						return (double)value;
					}
				}, env).getUndirected();
				
				

				Graph<Integer, Double, Double> result = graph.runVertexCentricIteration(new HashMinComputeFunction(),
						null, Integer.MAX_VALUE);

				// Extract the vertices as the result
				DataSet<Vertex<Integer, Double>> singleSourceShortestPaths = result.getVertices();

				singleSourceShortestPaths.print();

			}

			public static final class HashMinComputeFunction extends ComputeFunction<Integer, Double, Double, Double> {

				public void compute(Vertex<Integer, Double> vertex, MessageIterator<Double> messages) {

					if(getSuperstepNumber()==1)
					{
						for (Edge<Integer, Double> e : getEdges()) {
							sendMessageTo(e.getTarget(), vertex.getValue());
						}
					}
					else
					{
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
