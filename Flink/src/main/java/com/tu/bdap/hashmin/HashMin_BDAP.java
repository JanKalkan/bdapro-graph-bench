package com.tu.bdap.hashmin;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;

import com.tu.bdap.utils.DataSetID;
import com.tu.bdap.utils.DatasetLoader;


//This algorithm computes connected components by sending the lowest vertex-id to all neighbours
public class HashMin_BDAP {

	public static void main(String[] args) throws Exception {

		// Initialize DataSetPath and DataSet
		String dataSetPath = "";
		int dataSet = -1;

		try {
			dataSetPath = args[0];
			dataSet=Integer.parseInt(args[1]);
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
		DataSet<Edge<Integer, Double>> edges = null;

		// Read DataSet with GraphData using GraphLoader Class
		switch (dataSet) {
		case DataSetID.USA:
			edges = DatasetLoader.loadUsaInt(dataSetPath, env);
			break;
		case DataSetID.TWITTER:
			edges = DatasetLoader.loadTwitterInt(dataSetPath, env);
			break;
		case DataSetID.FRIENDSTER:
			edges = DatasetLoader.loadFriendsterInt(dataSetPath, env);
			break;
		}
		
		//Check if DataSet could be loaded!
		if(edges==null)
		{
			System.err.println("DataSet could not be loaded!");
			return;
		}

		// Create Graph from DataSet and make it undirected
		Graph<Integer, Double, Double> graph = Graph.fromDataSet(edges, new MapFunction<Integer, Double>() {
			@Override
			public Double map(Integer value) throws Exception {
				return (double) value;
			}
		}, env).getUndirected();

		// Run HashMin Algorithm on the Graph
		Graph<Integer, Double, Double> result = graph.runVertexCentricIteration(new HashMinComputeFunction(), null,
				numIterations);

		// Extract the vertices as the result
		DataSet<Vertex<Integer, Double>> hashmin = result.getVertices();

		hashmin.collect();

	}

	public static final class HashMinComputeFunction extends ComputeFunction<Integer, Double, Double, Double> {

		@Override
		public void compute(Vertex<Integer, Double> vertex, MessageIterator<Double> messages) {

			// First superstep: send vertex-id to each neighbour
			if (getSuperstepNumber() == 1) {
				for (Edge<Integer, Double> e : getEdges()) {
					sendMessageTo(e.getTarget(), vertex.getValue());
				}
				// All other supersteps:
			} else {
				// Find the smallest vertex-id which was recived
				double minMessage = Double.POSITIVE_INFINITY;
				for (Double msg : messages) {
					minMessage = Math.min(minMessage, msg);
				}
				// If the smallest vertex-id which was recived is smaller than the
				// current one, update it and send to all neighbours again
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
