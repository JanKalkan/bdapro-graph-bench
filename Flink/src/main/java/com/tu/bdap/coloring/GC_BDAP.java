package com.tu.bdap.coloring;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.*;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.NullValue;

import com.tu.bdap.utils.DataSetID;

import java.util.Random;

import static java.lang.Math.pow;

/**
 * Graph Colouring assigns each vertex a colour, <br>
 * so all neighbouring vertices have different colours. <br>
 * Vertices randomly decide to pick a colour. <br>
 * If neighbouring vertices simultaneously pick a colour, <br>
 * the one with the smallest ID gets priority
*/
public class GC_BDAP {
	/**
	 * Loads a graph from local disk or hdfs and executes Graph Colouring 
	 * @param args args[0] should contain path, args[1] is an integer identifying the dataset
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

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
		DataSet<Tuple2<Long, Long>> edges = null;

		// Read DataSet with GraphData using GraphLoader Class
		switch (dataSet) {
		case DataSetID.USA:
			edges = env.readTextFile(dataSetPath).filter(new FilterFunction<String>() {
				@Override
				public boolean filter(String s) throws Exception {
					return s.startsWith("a");
				}
			}).map(new MapFunction<String, Tuple2<Long, Long>>() {
				@Override
				public Tuple2<Long, Long> map(String value) throws Exception {
					String[] s = value.split(" ");
					return new Tuple2<>(Long.parseLong(s[1]), Long.parseLong(s[2]));
				}
			});
			break;
		case DataSetID.TWITTER:
			edges = env.readTextFile(dataSetPath).filter(new FilterFunction<String>() {
				@Override
				public boolean filter(String s) throws Exception {
					return Character.isDigit(s.charAt(0));
				}
			}).map(new MapFunction<String, Tuple2<Long, Long>>() {
				@Override
				public Tuple2<Long, Long> map(String value) throws Exception {
					String[] s = value.split(" ");
					return new Tuple2<>(Long.parseLong(s[0]), Long.parseLong(s[1]));
				}
			});
			break;
		case DataSetID.FRIENDSTER:
			edges = env.readTextFile(dataSetPath).filter(new FilterFunction<String>() {
				@Override
				public boolean filter(String s) throws Exception {
					return Character.isDigit(s.charAt(0));
				}
			}).map(new MapFunction<String, Tuple2<Long, Long>>() {
				@Override
				public Tuple2<Long, Long> map(String value) throws Exception {
					String[] s = value.split("\\s+");
					return new Tuple2<>(Long.parseLong(s[0]), Long.parseLong(s[1]));
				}
			});
			break;
		}

		// Check if DataSet could be loaded!
		if (edges == null) {
			System.err.println("DataSet could not be loaded!");
			return;
		}

		// Create Graph from DataSet
		Graph graph = Graph.fromTuple2DataSet(edges, env)
				.mapVertices(new MapFunction<Vertex<Long, NullValue>, Tuple2<Integer, Integer>>() {
					@Override
					public Tuple2<Integer, Integer> map(Vertex<Long, NullValue> longNullValueVertex) throws Exception {
						return new Tuple2<>(-1, -1);
					}
				});

		// transform the graph into an undirected one
		Graph gc = graph.getUndirected();

		// execute vertex-centric iteration
		Graph<Long, Tuple2<Integer, Integer>, Double> colour = gc.runVertexCentricIteration(new ComputeGC(),
				new CombineGC(), numIterations);

		colour.getVertices().print();

	}


	/**
	 * The Compute-function randomly assigns colours to the given vertex <br>
	 * and sends it status to the neighbor vertices. Each vertex stores
	 * two values: the assigned colour and the status. <br>
	 * Unassigned vertices have status -1, <br>
	 * tentative vertices have status 0, <br>
	 * assigned vertices have status 1 <br>
	 */
	public static final class ComputeGC
			extends ComputeFunction<Long, Tuple2<Integer, Integer>, Double, Tuple2<Long, Integer>> {

		/**
		 * During each superstep randomly decide to pick a colour and  give yourself a tentative status <br>
		 * If neighbours decide simultaneously to pick a colour, the vertex with the lowest id has priority 
		 * @param vertex The specific vertex, on which the Compute-function is called, the tuple contains colour and status of the vertex
		 * @param messages received messages from previous superstep
		 */
		public void compute(Vertex<Long, Tuple2<Integer, Integer>> vertex,
				MessageIterator<Tuple2<Long, Integer>> messages) {

			if (vertex.getValue().f1 == 1) {
				return;
			}
			Long min = Long.MAX_VALUE;
			int aliveNeighbours = 0;
			
			// count neighbours that don't have an assigned colour
			for (Tuple2<Long, Integer> msg : messages) {
				if (msg.f0 < min) {
					min = msg.f0;
				}
				if (msg.f1 == 1) {
					aliveNeighbours += msg.f1;
				}
			}

			Random r = new Random();
			int colour = getSuperstepNumber();
			int status = -1;
			if (getSuperstepNumber() == 1) {
				sendMessageToAllNeighbors(new Tuple2<>(Long.MAX_VALUE, 1));
				return;
			}
			int neighbours = aliveNeighbours;

			// if there are no neighbours left assign a colour
			if (neighbours == 0) {
				status = 1;
				setNewVertexValue(new Tuple2<>(colour, status));
				return;
			}

			// resolve conflict
			if (vertex.getValue().f1 == 0 & getSuperstepNumber() > 2) {
				// if all neighbouring nodes have a higher id,
				// then the current node has the right of way to pick the colour
				if (min > vertex.getId()) {
					colour = vertex.getValue().f0;
					setNewVertexValue(new Tuple2<>(colour, 1));
					sendMessageToAllNeighbors(new Tuple2<>(Long.MAX_VALUE, 0));
					return;
				} 
				else {
					setNewVertexValue(new Tuple2<>(colour, 0));
					sendMessageToAllNeighbors(new Tuple2<>(vertex.getId(), 1));
					return;
				}
			}

			// select tentative node randomly, pick current superstep as colour
			if (r.nextDouble() <= 1.0 / (2. * neighbours)) {
				setNewVertexValue(new Tuple2<>(colour, 0));
				sendMessageToAllNeighbors(new Tuple2<>(vertex.getId(), 1));
				return;

			}
			setNewVertexValue(new Tuple2<>(-1, -1));
			sendMessageToAllNeighbors(new Tuple2<>(Long.MAX_VALUE, 1));

		}
	}

	/**
	 * The Message Combiner for Graph Colouring sums all active neighbours and <br>
	 * keeps track of the smallest vertex id. The combiner reduces network traffic among cluster nodes.
	 */
	public static final class CombineGC extends MessageCombiner<Long, Tuple2<Long, Integer>> {

		public void combineMessages(MessageIterator<Tuple2<Long, Integer>> messages) {

			Long min = Long.MAX_VALUE;
			int neighbours = 0;
			for (Tuple2<Long, Integer> msg : messages) {
				neighbours += msg.f1;
				if (msg.f0 < min) {
					min = msg.f0;
				}

			}
			sendCombinedMessage(new Tuple2<Long, Integer>(min, neighbours));
		}
	}
}