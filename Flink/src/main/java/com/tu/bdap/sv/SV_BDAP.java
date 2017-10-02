package com.tu.bdap.sv;

import java.util.Iterator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import com.tu.bdap.utils.DataSetID;

/**
 * The Shiloach-Vishkin's Algorithm (SV) calculates connected components (CC)
 * by creating trees for each CC. 
 */
public class SV_BDAP {

    //Constants used for message interpretation
    final static Integer NULL  = 0;
    final static Integer REQUEST_PARENT_VERTX_ID  = 1;
    final static Integer RESPONSE_PARENT_VERTX_ID = 2;
    final static Integer REQUEST_PARENT_STAR_FLAG = 3;
    final static Integer NOT_IN_A_STAR            = 4;
    final static Integer RESPONSE_STAR_FLAG_FALSE = 5;
    final static Integer RESPONSE_STAR_FLAG_TRUE  = 6;
    final static Integer INFO_OF_PARENT_ID        = 7;
    final static Integer NEW_PARENT_ID            = 8;

    /**
     * Main method for executing the SV algorithm
     * @param args
     */
    public static void main(String[] args) throws Exception {
    	
    	int numIterations = 20;
 	
    	Graph<Integer, Tuple2<Integer, Boolean>, NullValue>  graph = loadGraph(args[0], Integer.parseInt(args[1]));

        graph = initializeParentVertices(graph);

        graph =  computeStarAssignment(graph);

        int count = 0;

        while (!allTreesAreStars(graph) && count < numIterations) {

            count++;

            graph = treeHooking(graph);

            graph = starHooking(graph);

            graph = shortCutting(graph);

            graph = computeStarAssignment(graph);

        }

        graph.getVertices().print();

    }//End: main()

    /**
     * Compute the star assignment for the current graph.
     * @param graph
     */
    public static Graph computeStarAssignment(Graph graph) {

        return graph.runVertexCentricIteration(new ComputeFunction<Integer, Tuple2<Integer, Boolean>, NullValue, Tuple2<Integer, Integer>>() {

            @Override
            public void compute(Vertex<Integer, Tuple2<Integer, Boolean>> vertex, MessageIterator<Tuple2<Integer, Integer>> messages) throws Exception {

                //Message structure: Tuple2<SourceVertexId, Integer used as generic value>
                switch (getSuperstepNumber()) {

                    //Initilaize star[u] flag to true and send request to vertex w = D[u] to receive D[w] in the next super step
                    case 1:
                        setNewVertexValue(new Tuple2<Integer, Boolean>(vertex.getValue().f0, true));
                        sendMessageTo(vertex.getValue().f0, new Tuple2<Integer, Integer>(vertex.getId(), REQUEST_PARENT_VERTX_ID));
                        break;

                    //Send back D[w], e.i. create the response for request of super step 1
                    case 2:
                        for (Tuple2<Integer, Integer> msg : messages) {
                            if (msg.f1 == REQUEST_PARENT_VERTX_ID) {
                                sendMessageTo(msg.f0, new Tuple2(vertex.getId(), vertex.getValue().f0));
                            }
                        }
                        break;

                    //Check whether w = D[u] is not equal to D[w], e.i. whether u is not in a star and propagate the result
                    case 3:
                        Boolean notInAStar = false;

                        for (Tuple2<Integer, Integer> msg : messages) {

                            if (vertex.getValue().f0 != msg.f1) {
                                notInAStar = true;
                                //Notify D[u]
                                sendMessageTo(vertex.getValue().f0, new Tuple2(vertex.getId(), NOT_IN_A_STAR));
                                //Notify D[w]
                                sendMessageTo(msg.f1, new Tuple2(vertex.getId(), NOT_IN_A_STAR));
                            }
                        }
                        //Set start flag to false
                        if (notInAStar) {
                            //Set star[u] flag to false
                            setNewVertexValue(new Tuple2<Integer, Boolean>(vertex.getValue().f0, false));
                        }
                        //Request D[u]
                        sendMessageTo(vertex.getValue().f0, new Tuple2(vertex.getId(), REQUEST_PARENT_STAR_FLAG));

                        break;

                    // Update the star flag if necessary [1] and send the current star flag to the requesting children [2]
                    case 4:
                        Integer response = vertex.getValue().f1 ? RESPONSE_STAR_FLAG_TRUE : RESPONSE_STAR_FLAG_FALSE;
                        // [1]
                        for (Tuple2<Integer, Integer> msg : messages) {
                            if (msg.f1 == NOT_IN_A_STAR) {
                                setNewVertexValue(new Tuple2<Integer, Boolean>(vertex.getValue().f0, false));
                                response = RESPONSE_STAR_FLAG_FALSE;
                                break;
                            }
                        }
                        // [2]
                        for (Tuple2<Integer, Integer> msg : messages) {
                            if (msg.f1 == REQUEST_PARENT_STAR_FLAG) {
                                sendMessageTo(msg.f0, new Tuple2<Integer, Integer>(vertex.getId(), response));
                            }
                        }
                        break;

                    // Finally set star[u] to star[w]
                    case 5:
                        for (Tuple2<Integer, Integer> msg : messages) {
                            Boolean star = (msg.f1 == RESPONSE_STAR_FLAG_TRUE) ? true : false;
                            setNewVertexValue(new Tuple2<Integer, Boolean>(vertex.getValue().f0, star));
                            break;
                        }
                        break;

                }

            }

        }, null, 5);

    }//End: computeStarAssignment()

    /**
     * Returns true if all star[v] is true for all v in V
     * @param graph 
     * @return boolean
     */
    public static boolean allTreesAreStars(Graph<Integer, Tuple2<Integer, Boolean>, NullValue> graph) throws Exception {

        DataSet vertices = graph.getVertices();

        vertices = vertices.filter(new FilterFunction<Vertex<Integer, Tuple2<Integer, Boolean>>>() {
            @Override
            public boolean filter(Vertex<Integer, Tuple2<Integer, Boolean>> vertex) throws Exception {
                if (!vertex.getValue().f1)
                    return true;
                else
                    return false;
            }
        });

        if (vertices.count() > 0) {
            return false;
        }

        return true;
    }//End: allTreesAreStars()

    /**
     * Executes the tree hooking for the current graph.
     * @param graph
     */
    public static Graph treeHooking(Graph graph) {

        return graph.runVertexCentricIteration(new ComputeFunction<Integer, Tuple2<Integer, Boolean>, NullValue, Tuple3<Integer, Integer, Integer>>() {

            @Override
            public void compute(Vertex<Integer, Tuple2<Integer, Boolean>> vertex, MessageIterator<Tuple3<Integer, Integer, Integer>> messages) throws Exception {

                //Message structure: Tuple3<SourceVertexId, Integer used as msg type, Integer used as generic msg value>

                switch (getSuperstepNumber()) {

                    //Request D[w] from w = D[u], e.i. request the vertex ID of the parent's parent
                    case 1:
                        sendMessageTo(vertex.getValue().f0, new Tuple3<Integer, Integer, Integer>(vertex.getId(), REQUEST_PARENT_VERTX_ID, NULL));
                        break;

                    //Respond to requests [1] from super step 1 and inform neighbours [2] about the own parent ID
                    case 2:
                        // [1]
                        for (Tuple3<Integer, Integer, Integer> msg : messages) {
                            if (msg.f1 == REQUEST_PARENT_VERTX_ID) {
                                sendMessageTo(msg.f0, new Tuple3<Integer, Integer, Integer>(vertex.getId(), RESPONSE_PARENT_VERTX_ID, vertex.getValue().f0));
                            }
                        }
                        // [2]
                            sendMessageToAllNeighbors(new Tuple3<Integer, Integer, Integer>(vertex.getId(), INFO_OF_PARENT_ID,vertex.getValue().f0));
                        break;

                    //Only execute if D[D[u]] == D[u], e.i. whether the parent of the parent is equal
                    case 3:
                        Integer grandParentID;
                        // Check the required condition D[D[u]] == D[u]
                        for (Tuple3<Integer, Integer, Integer> msg : messages) {
                            if (msg.f1 == RESPONSE_PARENT_VERTX_ID) {
                                grandParentID = msg.f2;
                                if (grandParentID != vertex.getValue().f0) {
                                    return;
                                }
                            }
                        }

                        //Initiate the tree hooking
                        for (Tuple3<Integer, Integer, Integer> msg : messages) {
                            //Check for arbitrary parent of neighbours, where the id is smaller than the own parent id
                            if (msg.f1 == INFO_OF_PARENT_ID && msg.f2 < vertex.getValue().f0 ) {
                                //Send message to parent to hook under the new parent ID => tree hooking
                                sendMessageTo(vertex.getValue().f0, new Tuple3<Integer, Integer, Integer>(vertex.getId(), NEW_PARENT_ID, msg.f2));
                                return;
                            }
                        }
                        break;

                    //Execute the actual tree hooking
                    case 4:
                        for (Tuple3<Integer, Integer, Integer> msg : messages) {
                            if (msg.f1 == NEW_PARENT_ID) {
                                setNewVertexValue(new Tuple2<Integer, Boolean>(msg.f2, vertex.getValue().f1));
                                break;
                            }
                        }
                        break;
                }

            }

        }, null, 4);

    }//End: treeHooking()

    /**
     * Executes the star hooking for the current graph.
     * @param graph
     */
    public static Graph starHooking(Graph graph) {

        return graph.runVertexCentricIteration(new ComputeFunction<Integer, Tuple2<Integer, Boolean>, NullValue, Tuple3<Integer, Integer, Integer>>() {

            @Override
            public void compute(Vertex<Integer, Tuple2<Integer, Boolean>> vertex, MessageIterator<Tuple3<Integer, Integer, Integer>> messages) throws Exception {

                //Message structure: Tuple3<SourceVertexId, Integer used as msg type, Integer used as generic msg value>

                switch (getSuperstepNumber()) {
                    //Inform neighbours about the own parent ID
                    case 1:
                        sendMessageToAllNeighbors(new Tuple3<Integer, Integer, Integer>(vertex.getId(), INFO_OF_PARENT_ID,vertex.getValue().f0));
                        break;

                    // For all vertices that are flagged as 'in a star' check whether there
                    // exists a parent with lower vertex ID than it's current parent
                    case 2:
                        if (vertex.getValue().f1 == false) {
                            return;
                        }

                        for (Tuple3<Integer, Integer, Integer> msg : messages) {
                            if (msg.f2 < vertex.getValue().f0) {
                                //Send message to parent to hook under the new parent ID => star hooking
                                sendMessageTo(vertex.getValue().f0, new Tuple3<Integer, Integer, Integer>(vertex.getId(), NEW_PARENT_ID, msg.f2));
                                return;
                            }
                        }
                        break;

                    //Execute the actual star hooking
                    case 3:
                        for (Tuple3<Integer, Integer, Integer> msg : messages) {
                            if (msg.f1 == NEW_PARENT_ID) {
                                setNewVertexValue(new Tuple2<Integer, Boolean>(msg.f2, vertex.getValue().f1));
                                break;
                            }
                        }
                        break;
                }

            }

        }, null, 3);

    }//End: starHooking()

    /**
     * Exectues the shortcutting on the current graph.
     * @param graph
     */
    public static Graph shortCutting(Graph graph) {

        return graph.runVertexCentricIteration(new ComputeFunction<Integer, Tuple2<Integer, Boolean>, NullValue, Tuple3<Integer, Integer, Integer>>() {

            @Override
            public void compute(Vertex<Integer, Tuple2<Integer, Boolean>> vertex, MessageIterator<Tuple3<Integer, Integer, Integer>> messages) throws Exception {

                //Message structure: Tuple3<SourceVertexId, Integer used as msg type, Integer used as generic msg value>

                switch (getSuperstepNumber()) {
                    //Request D[w] from w = D[u], e.i. request the vertex ID of the parent's parent
                    case 1:
                        sendMessageTo(vertex.getValue().f0, new Tuple3<Integer, Integer, Integer>(vertex.getId(), REQUEST_PARENT_VERTX_ID, NULL));
                        break;
                    //Respond to requests [1] from super step 1 and inform neighbours [2] about the own parent ID
                    case 2:
                        // [1]
                        for (Tuple3<Integer, Integer, Integer> msg : messages) {
                            if (msg.f1 == REQUEST_PARENT_VERTX_ID) {
                                sendMessageTo(msg.f0, new Tuple3<Integer, Integer, Integer>(vertex.getId(), RESPONSE_PARENT_VERTX_ID, vertex.getValue().f0));
                            }
                        }
                        break;
                    //Execute the actual short cutting
                    case 3:
                        for (Tuple3<Integer, Integer, Integer> msg : messages) {
                            if (msg.f1 == RESPONSE_PARENT_VERTX_ID) {
                                setNewVertexValue(new Tuple2<Integer, Boolean>(msg.f2, vertex.getValue().f1));
                            }
                        }
                        break;
                }
            }

        }, null, 3);

    }//End: shortCutting()

    /**
     * Initializes the parent vertex value, such that each vertex is the parent <br>  
     * of itself or a setting the neighbor as parent if the neighbor's vertex ID <br>
     * is smaller than the one of the respective vertex.
     * @param graph
     */
    public static Graph initializeParentVertices(Graph graph) {
        return graph.runVertexCentricIteration(new ComputeFunction<Integer, Tuple2<Integer, Boolean>, NullValue, Tuple3<Integer, Integer, Integer>>() {

            @Override
            public void compute(final Vertex<Integer, Tuple2<Integer, Boolean>> vertex, MessageIterator<Tuple3<Integer, Integer, Integer>> messages) throws Exception {

                //Message structure: Tuple3<SourceVertexId, Integer used as msg type, Integer used as generic msg value>
                Iterator<Edge<Integer, NullValue>> edges = getEdges().iterator();

                while (edges.hasNext()) {
                    Edge<Integer, NullValue> edge = edges.next();
                    if (vertex.getId() > edge.getTarget()) {
                        setNewVertexValue(new Tuple2<Integer, Boolean>(edge.getTarget(), vertex.getValue().f1));
                        break;
                    }
                }
            }
        }, null, 1);

    }//End: initializeParent()



    /**
     * Create a graph such that, each vertex v keeps two fields: D[u] and Γout(u), where D[u] points
     * to the parent of u in the tree and is initialized as u (i.e., forming a self loop at u).
     */
    private static Graph<Integer, Tuple2<Integer, Boolean>, NullValue> loadGraph(String path, int datasetID) {

            // Get execution environment
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            DataSet<Edge<Integer, NullValue>> edges = null;
            
            switch (datasetID) {
				case DataSetID.USA:
					edges = env.readTextFile(path)
	                .filter(new FilterFunction<String>() {
	                    @Override
	                    public boolean filter(String value) throws Exception {
	                        return value.startsWith("a");
	                    }
	                }).flatMap(new FlatMapFunction<String, Edge<Integer, NullValue>>() {
	                    @Override
	                    public void flatMap(String value, Collector<Edge<Integer, NullValue>> out) throws Exception {
	                        String[] values = value.split(" ");
	                        out.collect(new Edge<Integer, NullValue>(Integer.parseInt(values[1]), Integer.parseInt(values[2]),
	                                new NullValue()));
	                    }
	                });
					break;

			case DataSetID.TWITTER:
				edges = env.readTextFile(path)
                .flatMap(new FlatMapFunction<String, Edge<Integer, NullValue>>() {
                    @Override
                    public void flatMap(String value, Collector<Edge<Integer, NullValue>> out) throws Exception {
                        String[] values = value.split(" ");
                        if (Character.isDigit(values[0].charAt(0))) {
                        	 out.collect(new Edge<Integer, NullValue>(Integer.parseInt(values[0]), Integer.parseInt(values[1]),
                                     new NullValue()));
						}              
                    }
                });
				break;
				
			case DataSetID.FRIENDSTER:
				edges = env.readTextFile(path)
                .flatMap(new FlatMapFunction<String, Edge<Integer, NullValue>>() {
                    @Override
                    public void flatMap(String value, Collector<Edge<Integer, NullValue>> out) throws Exception {
                        String[] values = value.split("\\s+");
                        if (Character.isDigit(values[0].charAt(0))) {
                        	 out.collect(new Edge<Integer, NullValue>(Integer.parseInt(values[0]), Integer.parseInt(values[1]),
                                     new NullValue()));
						}              
                    }
                });
				break;
			}

            //Create graph and initialize the vertex value
            Graph<Integer, Tuple2<Integer, Boolean>, NullValue> graph = Graph.fromDataSet(edges, new MapFunction<Integer, Tuple2<Integer, Boolean>>() {
                @Override
                public Tuple2<Integer, Boolean> map(Integer vertexKey) throws Exception {
                    return new Tuple2<Integer, Boolean>(vertexKey, false);
                }
            }, env).getUndirected();

            return graph;
        }

}
