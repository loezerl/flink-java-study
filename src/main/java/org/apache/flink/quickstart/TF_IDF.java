package org.apache.flink.quickstart;

/**
 * Created by loezer on 02/06/17.
 */

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import sun.awt.Mutex;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Semaphore;

public class TF_IDF {

    static Map<String, Integer> MapWords;

    public static long cont_colecao =0;

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        MapWords = new Hashtable<String, Integer>(); //Numero de vzes em que a palavra aparece

        // get input data
//		DataSet<String> text = env.fromElements(
//				"Lucas Loezer",
//				"Lucas Fabre",
//				"Lucas Lucas",
//				"AAA BBB CCC A B C A B C A B C"
//				);
        // -> Arrumar
//        - Tentar armazenar o numero de documentos que o termo aparece em um Dataset
//        - Por alguma razao ele executa mais de uma vez o linesplitter
        DataSet<String> text = env.readTextFile("/home/loezer/flink/flink-java-project/teste3.txt");
        cont_colecao = text.count();
        DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new LineSplitter())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);

        // execute and print result
//        System.out.println(Arrays.asList(MapWords));
//        System.out.println("Quantidade de linhas: ");
//        System.out.println(cont_colecao);
//        System.out.println("======================================");
//        System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n");

        DataSet<Tuple3<String,Integer, Double>> tfidf = counts.flatMap(new TFIDF());

        counts.print();
        tfidf.print();

//        env.execute();
    }

    //
    // 	User Functions
    // FlatMapFunction<Tipo Entrada, Tipo Saida>
    public static final class TFIDF implements FlatMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Double>>{
        @Override
        public void flatMap(Tuple2<String, Integer> in, Collector<Tuple3<String, Integer, Double>> out){
            Double tfidf = 0.0;
            tfidf = in.f1 * (Math.log10(cont_colecao) / MapWords.get(in.f0));
            out.collect(new Tuple3<String,Integer, Double>(in.f0, MapWords.get(in.f0), tfidf));
        }
    }
    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
     */
    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");
            Map<String, Integer>LineMap = new HashMap<String, Integer>();

            // emit the pairs
            for (String token : tokens) {
                    synchronized (MapWords) {
                        if (!MapWords.containsKey(token)) {
                            MapWords.put(token, 1);
                            LineMap.put(token, 0);
                        } else if (!LineMap.containsKey(token)) {
                            MapWords.put(token, MapWords.get(token) + 1);
                            LineMap.put(token, 0);
                        }
                    }
                    if (token.length() > 0) {
                        out.collect(new Tuple2<String, Integer>(token, 1));
                    }

            }
        }
    }
}
