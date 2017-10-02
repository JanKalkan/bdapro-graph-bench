package com.tu.bdap.utils;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.util.Collector;

/**
 * This class loads Graph-DataSets from HDFS or local disk storage
 */
public class DatasetLoader {
	
	
	public static DataSet<Edge<Integer, Double>> loadUsaInt(String path, ExecutionEnvironment env)
	{
		
		DataSet<Edge<Integer, Double>> edges = env.readTextFile(path).filter(new FilterFunction<String>() {
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
				

				
				return edges;
	}
	
	/**
	 * 	Load USA dataset
	 * @param path path to dataset
	 * @param env Execution environment 
	 * @return Edges of resulting graph
	 */
	public static DataSet<Edge<Long, Double>> loadUsaLong(String path, ExecutionEnvironment env)
	{
		
		DataSet<Edge<Long, Double>> edges = env.readTextFile(path).filter(new FilterFunction<String>() {
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
				

				
				return edges;
		
	}
	
	/**
	 * 	Load LiveJournal dataset
	 * @param path path to dataset
	 * @param env Execution environment 
	 * @return Edges of resulting graph
	 */
	public static DataSet<Edge<String, String>> loadLiveJournal(String path, ExecutionEnvironment env)
	{
		
		DataSet<Edge<String, String>> edges = env.readTextFile(path).filter(new FilterFunction<String>() {
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
				

				
				return edges;
		
	}
	/**
	 * 	Load Twitter dataset with Long IDs
	 * @param path path to dataset
	 * @param env Execution environment 
	 * @return Edges of resulting graph
	 */
	public static DataSet<Edge<Long, Double>> loadTwitterLong(String path, ExecutionEnvironment env)
	{
		
				DataSet<Edge<Long, Double>> edges = env.readTextFile(path).filter(new FilterFunction<String>() {
					@Override
					public boolean filter(String value) throws Exception {
						return Character.isDigit(value.charAt(0));
					}
				}).flatMap(new FlatMapFunction<String, Edge<Long, Double>>() {
					@Override
					public void flatMap(String value, Collector<Edge<Long, Double>> out) throws Exception {
						String[] values = value.split(" ");
						out.collect(new Edge<Long, Double>(Long.parseLong(values[0]), Long.parseLong(values[1]),
								1.0));
					}
				});
				

				
				return edges;
		
	}
	
	/**
	 * 	Load Twitter dataset with Integer IDs
	 * @param path path to dataset
	 * @param env Execution environment 
	 * @return Edges of resulting graph
	 */
	public static DataSet<Edge<Integer, Double>> loadTwitterInt(String path, ExecutionEnvironment env)
	{
		
		DataSet<Edge<Integer, Double>> edges = env.readTextFile(path).filter(new FilterFunction<String>() {
			@Override
			public boolean filter(String value) throws Exception {
				return Character.isDigit(value.charAt(0));
			}
		}).flatMap(new FlatMapFunction<String, Edge<Integer, Double>>() {
			@Override
			public void flatMap(String value, Collector<Edge<Integer, Double>> out) throws Exception {
				String[] values = value.split(" ");
				out.collect(new Edge<Integer, Double>(Integer.parseInt(values[0]), Integer.parseInt(values[1]),
						1.0));
			}
		});
				

				
				return edges;
		
	}
	/**
	 * 	Load Friendster dataset with Integer IDs
	 * @param path path to dataset
	 * @param env Execution environment 
	 * @return Edges of resulting graph
	 */
	public static DataSet<Edge<Integer, Double>> loadFriendsterInt(String path, ExecutionEnvironment env)
	{
		
		DataSet<Edge<Integer, Double>> edges = env.readTextFile(path).filter(new FilterFunction<String>() {
			@Override
			public boolean filter(String value) throws Exception {
				return Character.isDigit(value.charAt(0));
			}
		}).flatMap(new FlatMapFunction<String, Edge<Integer, Double>>() {
			@Override
			public void flatMap(String value, Collector<Edge<Integer, Double>> out) throws Exception {
				String[] values = value.split("\\s+");
				out.collect(new Edge<Integer, Double>(Integer.parseInt(values[0]), Integer.parseInt(values[1]),
						1.0));
			}
		});
				

				
				return edges;
		
	}
	/**
	 * 	Load Friendster dataset with Long IDs
	 * @param path path to dataset
	 * @param env Execution environment 
	 * @return Edges of resulting graph
	 */
	public static DataSet<Edge<Long, Double>> loadFriendsterLong(String path, ExecutionEnvironment env)
	{
		
				DataSet<Edge<Long, Double>> edges = env.readTextFile(path).filter(new FilterFunction<String>() {
					@Override
					public boolean filter(String value) throws Exception {
						return Character.isDigit(value.charAt(0));
					}
				}).flatMap(new FlatMapFunction<String, Edge<Long, Double>>() {
					@Override
					public void flatMap(String value, Collector<Edge<Long, Double>> out) throws Exception {
						String[] values = value.split("\\s+");
						out.collect(new Edge<Long, Double>(Long.parseLong(values[0]), Long.parseLong(values[1]),
								1.0));
					}
				});
				

				
				return edges;
		
	}
	
	

}
