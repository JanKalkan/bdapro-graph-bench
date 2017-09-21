package com.tu.bdap.bmm;

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

//This algorithm computes a bipartit maximal matching
public class BMM_BDAP {

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
		int numIterations = 20;

		// Create execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Initialize DataSet with GraphData
		DataSet<Edge<String, String>> edges = null;

		// Read DataSet with GraphData using GraphLoader Class
		switch (dataSet) {
		case DataSetID.LIVEJOURNAL:
			edges = DatasetLoader.loadLiveJournal(dataSetPath, env);
			break;
		}
		// Check if DataSet could be loaded!
		if (edges == null) {
			System.err.println("DataSet could not be loaded!");
			return;
		}

		@SuppressWarnings("serial")
		Graph<String, String, String> graph = Graph.fromDataSet(edges, new MapFunction<String, String>() {

			@Override
			public String map(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return "-1";
			}
		}, env);

		//Run BMMComputeFunction
		graph = graph.runVertexCentricIteration(new BMMComputeFunction(), null, numIterations);

		graph.getVertices().collect();

	}

	@SuppressWarnings("serial")
	public static final class BMMComputeFunction extends ComputeFunction<String, String, String, String> {

		@Override
		public void compute(Vertex<String, String> vertex, MessageIterator<String> messages) {

			// First Superstep
			if ((getSuperstepNumber() % 4) == 1 && vertex.getValue().equals("-1")) {
				// Check if vertex got matched in the last Superstep
				boolean c = false;
				for (String msg : messages) {
					if (!msg.equals("-1")) {
						setNewVertexValue(msg);
						c = true;
					}
				}
				if (!c) {
					for (Edge<String, String> e : getEdges()) {
						sendMessageTo(e.getTarget(), vertex.getId());
					}
				}
			}

			// Second Superstep
			if ((getSuperstepNumber() % 4) == 2) {

				boolean a = false;

				if (vertex.getValue().equals("-1")) {
					for (String msg : messages) {
						if (!a) {
							sendMessageTo(msg, vertex.getId());
							a = true;
						} else {
							sendMessageTo(msg, "-1");
						}
					}
				}

			}

			// Third Superstep
			if ((getSuperstepNumber() % 4) == 3) {
				boolean b = false;
				for (String msg : messages) {
					if (!b && !msg.equals("-1")) {
						// setNewVertexValue(msg);
						sendMessageTo(msg, vertex.getId());
						b = true;
					} else {
						sendMessageTo(vertex.getId(), "-1");
					}
				}

			}

			// Fourth Superstep
			if ((getSuperstepNumber() % 4) == 0) {
				if (vertex.getId().endsWith("R")) {
					String m = messages.next();
					setNewVertexValue(m);
					sendMessageTo(m, vertex.getId());

				} else {
					sendMessageTo(vertex.getId(), "-1");
				}

			}

		}
	}

}