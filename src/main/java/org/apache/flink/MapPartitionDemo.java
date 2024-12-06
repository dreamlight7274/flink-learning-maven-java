package org.apache.flink;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MapPartitionDemo {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> textLines = env.fromElements("BMW", "Tesla", "Rolls-Royce");
        DataSet<Long> counts = textLines.mapPartition(new PartitionCounter());
        counts.print();
    }
    // map是针对数据集中的单独元素，但mapPartition是针对数据集中的每个分区中的元素，单位不一样，一个是以每个元素为单位的，
    // 但另一个是以一个分区的元素为单位的，这免去了许多重复的操作
    private static class PartitionCounter implements MapPartitionFunction<String, Long>{
        @Override
        public void mapPartition(Iterable<String> values, Collector<Long> out) throws Exception{
            long i = 0;
            for(String value: values){
                i++;
            }
            out.collect(i);

        }
    }

}

