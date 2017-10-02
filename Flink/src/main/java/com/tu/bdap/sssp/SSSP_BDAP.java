package com.tu.bdap.sssp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;

import com.tu.bdap.utils.DataSetID;
import com.tu.bdap.utils.DatasetLoader;

/**
 * This algorithm computes the shortest path to all other nodes from one source vertex
 */
public class SSSP_BDAP {

	/**
	 * Loads a graph from local disk or hdfs and calculates the shortest path to all vertices given a source
	 * @param args args[0] should contain path, args[1] is an integer identifying the dataset
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		// Define scrVertexID
		final long srcVertexId = 101;

		// Initialize DataSetPath and DataSet
		String dataSetPath = "";
		int dataSet = -1;

		try {
			dataSetPath = args[0];
			dataSet = Integer.parseInt(args[1]);
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (dataSetPath == "" || dataSet == -1) {
			System.err.println("Wrong input parameters!");
			return;
		}

		// Iterations
		int numIterations = 11;

		// Create execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Initialize DataSet with GraphData
		DataSet<Edge<Long, Double>> edges = null;

		// Read DataSet with GraphData using GraphLoader Class
		switch (dataSet) {
		case DataSetID.USA:
			edges = DatasetLoader.loadUsaLong(dataSetPath, env);
			break;
		case DataSetID.TWITTER:
			edges = DatasetLoader.loadTwitterLong(dataSetPath, env);
			break;
		case DataSetID.FRIENDSTER:
			edges = DatasetLoader.loadFriendsterLong(dataSetPath, env);
			break;
		}

		// Check if DataSet could be loaded!
		if (edges == null) {
			System.err.println("DataSet could not be loaded!");
			return;
		}

		// Create Graph from DataSet
		Graph<Long, Double, Double> graph = Graph.fromDataSet(edges, new MapFunction<Long, Double>() {
			@Override
			public Double map(Long value) throws Exception {
				return Double.POSITIVE_INFINITY;
			}
		}, env);

		// Run SSSP compute function
		Graph<Long, Double, Double> result = graph.runVertexCentricIteration(new SSSPComputeFunction(srcVertexId),
				new SSSPCombiner(), numIterations);
		// Extract the vertices as the result
		DataSet<Vertex<Long, Double>> singleSourceShortestPaths = result.getVertices();

		singleSourceShortestPaths.collect();

	}

	/**
	 * The Compute-Function calculates the distance to a certain node
	 * given the information of its neighbours.
	 *
	 */
	@SuppressWarnings("serial")
	public static final class SSSPComputeFunction extends ComputeFunction<Long, Double, Double, Double> {

		private final long srcId;

		public SSSPComputeFunction(long src) {
			this.srcId = src;
		}

		/**
		 * Each vertex calculates the distance to the source vertex.
		 * Whenever a neghbour sends a shorter distance, update the currently stored distance. 
		 * @param vertex current vertex
		 * @param messages incoming messages that contain distance to source node
		 */
		@Override
		public void compute(Vertex<Long, Double> vertex, MessageIterator<Double> messages) {

			//Set POSITIVE_INFINITY as distance to the other nodes
			double minDistance = (vertex.getId().equals(srcId)) ? 0d : Double.POSITIVE_INFINITY;

			//check if recived distance is smaller than current one
			for (Double msg : messages) {
				minDistance = Math.min(minDistance, msg);
			}

			//if recived distance is smaller than current one, update it!
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