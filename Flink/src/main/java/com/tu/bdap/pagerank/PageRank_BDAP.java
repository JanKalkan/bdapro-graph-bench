package com.tu.bdap.pagerank;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.PageRank;

import com.tu.bdap.utils.DataSetID;
import com.tu.bdap.utils.DatasetLoader;

//This algorithm computes the importance of every node in a graph
public class PageRank_BDAP {

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

		// Check if DataSet could be loaded!
		if (edges == null) {
			System.err.println("DataSet could not be loaded!");
			return;
		}

		// Create Graph from DataSet
		Graph<Integer, Double, Double> graph = Graph.fromDataSet(edges, new MapFunction<Integer, Double>() {
			@Override
			public Double map(Integer value) throws Exception {
				return 1.0;
			}
		}, env);

		// Run PageRank provided by Flink Gelly
		PageRank<Integer> pr = new PageRank<>(0.85, numIterations);
		pr.run(graph).collect();
	}

}