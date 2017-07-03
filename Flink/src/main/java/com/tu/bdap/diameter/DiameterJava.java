package Graphs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.NullValue;

import java.util.Random;

import static java.lang.Math.pow;


/**
 * Created by simon on 03.07.17..
 */
public class DiameterJava {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSet<Tuple2<Long,Long>> edges = env.readTextFile("/home/simon/GraphAls/src/main/scala/Graphs/nodes.csv")
                .map(new MapFunction<String, Tuple2<Long,Long>>(){
                    @Override
                    public Tuple2<Long, Long> map(String value) throws Exception {
                        String[] s =value.split(",");
                        return new Tuple2<>(Long.parseLong(s[0]),Long.parseLong(s[1]));
                    }
                });

        int k =20;
        Graph graph = Graph.fromTuple2DataSet(edges,env)
                .mapVertices(new MapFunction<Vertex<Long,NullValue>, Tuple4<Long,Long,Long,Integer>>() {
                    @Override
                    public Tuple4 map(Vertex<Long, NullValue> value) throws Exception {
                        Random r = new Random();
                        Long s1 = 0L;
                        Long s2 = 0L;
                        Long s3 = 0L;
                        for (int i = 0; i<=20; i++){
                            if (r.nextDouble()<= pow(2,-(i+1))) s1= s1 | ((int)pow(2,i));
                            if (r.nextDouble()<= pow(2,-(i+1))) s2= s2 | ((int)pow(2,i));
                            if (r.nextDouble()<= pow(2,-(i+1))) s3= s3 | ((int)pow(2,i));
                        }
                        return new Tuple4(s1,s2,s3,0);
                    }
                });

        graph.getVertices().print();
        graph.runVertexCentricIteration(new ComputeDiameter(), new CombineDiameter(), 20 )
        .getVertices().print();
    }
    public static final class ComputeDiameter extends ComputeFunction<Long, Tuple4<Long,Long,Long,Integer>, NullValue, Tuple4<Long,Long,Long,Integer>> {

        public void compute(Vertex<Long, Tuple4<Long, Long, Long, Integer>> vertex, MessageIterator<Tuple4<Long, Long, Long, Integer>> messages) {

            Tuple4<Long, Long, Long, Integer> vert = vertex.getValue().copy();
            Long s0 = vert.f0;
            Long s1 = vert.f1;
            Long s2 = vert.f2;
            int diam = vert.f3;
            Tuple4 t = new Tuple4(0L, 0L, 0L, 0);
            for (Tuple4<Long, Long, Long, Integer> msg : messages) {
                s0 = s0 | msg.f0;
                s1 = s1 | msg.f1;
                s2 = s2 | msg.f2;
                diam = Math.max(diam,msg.f3);
            }

            setNewVertexValue(new Tuple4<>(s0,s1,s2,diam));
                for (Edge<Long, NullValue> e : getEdges()) {
                    sendMessageTo(e.getTarget(), vertex.getValue().copy());
                }
            }
        }

    public static final class CombineDiameter  extends MessageCombiner<Long, Tuple4<Long, Long, Long, Integer>> {

        public void combineMessages(MessageIterator<Tuple4<Long, Long, Long, Integer>> messages) {

            Tuple4<Long, Long, Long, Integer> t = new Tuple4(0L, 0L, 0L, 0);
            for (Tuple4<Long, Long, Long, Integer> msg: messages) {
                t.f0 = t.f0 | msg.f0;
                t.f1 = t.f1 | msg.f1;
                t.f2 = t.f2 | msg.f2;
                t.f3 = Math.max(t.f3,msg.f3);
            }
            sendCombinedMessage(t);
        }
    }
    }
