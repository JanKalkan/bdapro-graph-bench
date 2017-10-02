package com.tu.bdap.diameter;

import static java.lang.Math.pow;

import java.util.Random;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.NullValue;

import com.tu.bdap.utils.DataSetID;

/** A Diameter describes the longest possible path between two vertices,<br>
 * assuming one takes always the shortest path. <br>
 * The standard implementation would store all reachable vertices<br>
 * in each superstep and send the set to all neighbours and thereby
 * increase these sets.
 * Since exact calculation is very memory-expensive, <br>
 * probabilistic counting is used to estimate those sets. <br>
 * @see <a href="http://www.cs.cmu.edu/~christos/PUBLICATIONS/kdd02-anf.pdf">http://www.cs.cmu.edu/~christos/PUBLICATIONS/kdd02-anf.pdf</a>
 */
public class Diameter_BDAP {
	/**
	 * Loads a graph from local disk or hdfs and calculates Diameter Estimation
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
		Graph graph = Graph.fromTuple2DataSet(edges, env);
		// graph = graph.getUndirected();
		
		// Since the diameter is hard to compute, random counting is used.
		// Each nodes stores three samples and the longest path
		final long k = 10L;
		graph = graph.mapVertices(new MapFunction<Vertex<Long, NullValue>, Tuple4<Long, Long, Long, Integer>>() {
			@Override
			public Tuple4 map(Vertex<Long, NullValue> value) throws Exception {
				Random r = new Random();
				Long s1 = 0L;
				Long s2 = 0L;
				Long s3 = 0L;
				for (int i = 0; i <= k; i++) {
					if (r.nextDouble() <= pow(2, -(i + 1)))
						s1 = s1 | ((long) pow(2, i));
					if (r.nextDouble() <= pow(2, -(i + 1)))
						s2 = s2 | ((long) pow(2, i));
					if (r.nextDouble() <= pow(2, -(i + 1)))
						s3 = s3 | ((long) pow(2, i));
				}

				return new Tuple4(s1, s2, s3, 0);
			}
		});
		// graph.getVertices().print();

		//execute vertex-centric iterations
		graph = graph.runVertexCentricIteration(new ComputeDiameter(), new CombineDiameter(), numIterations);

		graph.getVertices().collect();
		// graph.getVertices().writeAsText(args[1]);
	}


	  /**
	    * Merge current estimated set with received sets by doing a logical OR. <br>
	    * If estimated set did not increase by a certain threshold, <br>
	    * stop calculation.
	    * 
	   */
	public static final class ComputeDiameter extends
			ComputeFunction<Long, Tuple4<Long, Long, Long, Integer>, NullValue, Tuple4<Long, Long, Long, Integer>> {

		double e = 0.10;

		/**
		 * Compute function of Diameter Estimation
		 * @param vertex current vertex with three random samples and the longest measured path
		 * @param messages received messages from
		 */
		
		@Override
		public void compute(Vertex<Long, Tuple4<Long, Long, Long, Integer>> vertex,
				MessageIterator<Tuple4<Long, Long, Long, Integer>> messages) {

			// merge bitstrings to estimate size
			Tuple4<Long, Long, Long, Integer> vert = vertex.getValue();
			Long s0 = vert.f0;
			Long s1 = vert.f1;
			Long s2 = vert.f2;
			int diam = vert.f3;
			Tuple4 t = new Tuple4(0L, 0L, 0L, 0);
			for (Tuple4<Long, Long, Long, Integer> msg : messages) {
				s0 = s0 | msg.f0;
				s1 = s1 | msg.f1;
				s2 = s2 | msg.f2;
				diam = Math.max(diam, msg.f3);
			}
			
			// calculate estimated size 
			double lowestBitOld = (lowestZero(vert.f0) + lowestZero(vert.f1) + lowestZero(vert.f2)) / 3;
			double lowestBitNew = (lowestZero(s0) + lowestZero(s1) + lowestZero(s2)) / 3;

			double N_old = pow(2, lowestBitOld) / 0.77351;
			double N_new = pow(2, lowestBitNew) / 0.77351;
			
			// if the estimated size does not change more than a given threshold,
			// stop computation. Otherwise update value and send current value to neighbours.
			if (N_new > N_old * (1.0 + e) || diam == 0) {
				diam += 1;
				setNewVertexValue(new Tuple4<>(s0, s1, s2, diam));
				sendMessageToAllNeighbors(new Tuple4<>(s0, s1, s2, diam));

			}

		}

	}

	/**
	 * The Message Combiner for Diameter Estimation merges the samples and keeps track of the longest path. 
	 * The combiner reduces network traffic among cluster nodes.
	 */
	public static final class CombineDiameter extends MessageCombiner<Long, Tuple4<Long, Long, Long, Integer>> {

		@Override
		public void combineMessages(MessageIterator<Tuple4<Long, Long, Long, Integer>> messages) {

			Tuple4<Long, Long, Long, Integer> t = new Tuple4(0L, 0L, 0L, 0);
			for (Tuple4<Long, Long, Long, Integer> msg : messages) {
				t.f0 = t.f0 | msg.f0;
				t.f1 = t.f1 | msg.f1;
				t.f2 = t.f2 | msg.f2;
				t.f3 = Math.max(t.f3, msg.f3);
			}

			sendCombinedMessage(t);
		}
	}

	/**
	 * Given a bit string calculate the index of the zero with the lowest index
	 * @param bits a Long representing a bit string
	 * @return index of rightmost zero
	 */
	public static double lowestZero(Long bits) {
		Long zero = bits | (bits + 1);
		Long difference = (bits ^ zero);

		double index = 64 - Long.numberOfLeadingZeros(difference);
		return index;
	}
}
