package org.apache.flink;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;

public class ReduceDemo {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<WC> words = env.fromElements(
                new WC("BMW", 1),
                new WC("Tesla", 1),
                new WC("Tesla", 9),
                new WC("Rolls-Royce", 1)
        );

        DataSet<WC> wordCounts = words.groupBy(
//                "word"
                new KeySelector<WC, String>() {
            @Override
            public String getKey(WC value) throws Exception{
                return value.word;
            }
        }
        ).reduce(new ReduceFunction<WC>() {
            @Override
            public WC reduce(WC wc, WC t1) throws Exception {
                return new WC(t1.word, wc.frequency+t1.frequency);
            }
        });

        wordCounts.print();
    }

    public static class WC {
        public String word;
        public int frequency;
        public WC() {

        }
        public WC(String word, int count){
            this.word = word;
            this.frequency = count;
        }

//        public String getWord() {
//            return word;
//        }
//        public int getCount() {
//            return count;
//        }

        @Override
        public String toString(){
            return "WC{" + "word='" +word +'\'' + ",count=" +frequency + '}';
        }
    }
}
