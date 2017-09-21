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

/**
 * Created by simon on 03.07.17..
 */
public class Diameter_BDAP {
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

		graph = graph.runVertexCentricIteration(new ComputeDiameter(), new CombineDiameter(), numIterations);

		graph.getVertices().collect();
		// graph.getVertices().writeAsText(args[1]);
	}

	public static final class ComputeDiameter extends
			ComputeFunction<Long, Tuple4<Long, Long, Long, Integer>, NullValue, Tuple4<Long, Long, Long, Integer>> {

		double e = 0.00;

		@Override
		public void compute(Vertex<Long, Tuple4<Long, Long, Long, Integer>> vertex,
				MessageIterator<Tuple4<Long, Long, Long, Integer>> messages) {

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
			double lowestBitOld = (lowestZero(vert.f0) + lowestZero(vert.f1) + lowestZero(vert.f2)) / 3;
			double lowestBitNew = (lowestZero(s0) + lowestZero(s1) + lowestZero(s2)) / 3;

			double N_old = pow(2, lowestBitOld) / 0.77351;
			double N_new = pow(2, lowestBitNew) / 0.77351;
			if (N_new > N_old * (1.0 + e) || diam == 0) {
				diam += 1;
				setNewVertexValue(new Tuple4<>(s0, s1, s2, diam));
				sendMessageToAllNeighbors(new Tuple4<>(s0, s1, s2, diam));

			}

		}

	}

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

	public static double lowestZero(Long bits) {
		Long zero = bits | (bits + 1);
		Long difference = (bits ^ zero);

		double index = 64 - Long.numberOfLeadingZeros(difference);
		return index;
	}
}
