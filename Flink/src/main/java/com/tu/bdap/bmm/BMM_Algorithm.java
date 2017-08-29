package com.tu.bdap.bmm;

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

public class BMM_Algorithm {

	public static void main(String[] args) throws Exception {
		// Create execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// LiveJournal Dataset
		@SuppressWarnings("serial")
		DataSet<Edge<String, String>> edges = env.readTextFile(args[0]).filter(new FilterFunction<String>() {
			@Override
			public boolean filter(String value) throws Exception {
				return Character.isDigit(value.charAt(0));
			}
		}).flatMap(new FlatMapFunction<String, Edge<String, String>>() {
			@Override
			public void flatMap(String value, Collector<Edge<String, String>> out) throws Exception {
				String[] values = value.split(" ");
				out.collect(new Edge<String, String>(values[0] + "L", values[1] + "R", ""));
			}
		});

		@SuppressWarnings("serial")
		Graph<String, String, String> graph = Graph.fromDataSet(edges, new MapFunction<String, String>() {

			@Override
			public String map(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return "-1";
			}
		}, env);

		graph = graph.runVertexCentricIteration(new BMMComputeFunction(), null, 20);

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