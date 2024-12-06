package org.apache.flink;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class DeltaIterationDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Long, Double>> initialSolutionSet = env.fromElements(
                // 为了明确数据类型，在数据后加后缀
                new Tuple2<Long, Double>(1l, 4.17d));
        DataSet<Tuple2<Long, Double>> initialDeltaSet = env.fromElements(
                new Tuple2<Long, Double>(2l, 1.9d),
                new Tuple2<Long, Double>(2l, 4.8d),
                new Tuple2<Long, Double>(3l, 2.9d)
        );
        DeltaIteration<Tuple2<Long, Double>, Tuple2<Long, Double>> iteration = initialSolutionSet
                .iterateDelta(initialDeltaSet, 100, 0);
        DataSet<Tuple2<Long, Double>> candidateUpdates = iteration.getWorkset().groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Long, Double>> values, Collector<Tuple2<Long, Double>> out) throws Exception {
                        Iterator<Tuple2<Long, Double>> ite = values.iterator();
                        while (ite.hasNext()) {
                            Tuple2<Long, Double> item = ite.next();
                            out.collect(new Tuple2<Long, Double>(item.f0, item.f1+1));
                        }
                    }
                });
        DataSet<Tuple2<Long, Double>> deltas = candidateUpdates.join(iteration.getSolutionSet())
                .where(0).equalTo(0).with(new FlatJoinFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public void join(Tuple2<Long, Double> first, Tuple2<Long, Double> second, Collector<Tuple2<Long, Double>> out) throws Exception {
                        if (second != null) {
                            out.collect(new Tuple2<Long, Double>(first.f0, first.f1+second.f1));
                        } else {
                            out.collect(first);
                        }
                    }
                });
        DataSet<Tuple2<Long, Double>> nextWorkSet = deltas.filter(i->i.f0<1);
        iteration.closeWith(deltas, nextWorkSet).print();

    }
}
