/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FlatMapFunction;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class
BatchJob {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//		DataSet<Integer> inputDataSet = env.fromElements(1, 2, 3, 4, 5);
//
//		// 使用 Map 算子对数据集进行转换，将每个整数加上 10，并输出结果
//		DataSet<Integer> outputDataSet = inputDataSet.map(new MapFunction<Integer, Integer>() {
//			@Override
//			public Integer map(Integer value) throws Exception {
//				// 对每个整数都加上 10，并将结果作为新的数据集
//				return value + 10;
//			}
//		});
//
//		// 输出转换后的数据集
//		outputDataSet.print();

		// 假设已经有一个包含文本行的数据集
//		DataSet<String> inputDataSet = env.fromElements(
//				"Flink is a powerful framework for stream and batch processing",
//				"It provides support for event time processing"
//		);
//
//		// 使用 FlatMap 算子对数据集进行拆分并生成单词列表
//		DataSet<String> wordDataSet = inputDataSet.flatMap(new FlatMapFunction<String, String>() {
//			@Override
//			public void flatMap(String value, Collector<String> out) throws Exception {
//				// 按空格拆分文本行，并将拆分后的单词逐个添加到输出集合
//				String[] words = value.split(" ");
//				for (String word : words) {
//					out.collect(word);
//				}
//			}
//		});
//
//		// 输出单词列表
//		wordDataSet.print();

//		DataSet<Integer> inputDataSet = env.fromElements(1,2,3,4,5,6,7,8,9,10);
//
//		DataSet<Integer> evenDataSet = inputDataSet.filter(new FilterFunction<Integer>() {
//			@Override
//			public boolean filter(Integer integer) throws Exception {
//				return integer % 2 == 0;
//			}
//		});
//
//		evenDataSet.print();

//		DataSet<Integer> inputDataSet = env.fromElements(1,2,3,4,5);
//		DataSet<Integer> resultDataSet = inputDataSet.reduce(new ReduceFunction<Integer>() {
//			@Override
//			public Integer reduce(Integer integer, Integer t1) throws Exception {
//				return integer + t1;
//			}
//		});
//		resultDataSet.print();

//		DataSet<String> text = env.fromElements(
//				"Flink batch demo",
//				"batch demo",
//				"demo"
//		);
//
//		DataSet<Tuple2<String, Integer>> ds  =text.flatMap(new LineSplitter())
//				.groupBy(0)
//				.sum(1);
//		ds.print();


//		DataSet<Tuple2<String, Double>> inputDataSet = env.fromElements(
//				new Tuple2("Zeus", 95.2),
//				new Tuple2("Oner", 95.3),
//				new Tuple2("Faker", 95.3),
//				new Tuple2("Gumayusi",95.4),
//				new Tuple2("Keria",95.3)
//		);

//		double avgScore = inputDataSet.aggregate(Aggregations.SUM, 1).div(inputDataSet.count());
//		AggregateOperator<Tuple2<String, Double>> avgScore = inputDataSet.aggregate(Aggregations.SUM, 1);



		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataSet<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/batch/index.html
		 *
		 * and the examples
		 *
		 * http://flink.apache.org/docs/latest/apis/batch/examples.html
		 *
		 */

		// execute program
//		env.execute("Flink Batch Java API Skeleton");

	}
//	static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
//		@Override
//		public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception{
//			for (String word: line.split(" ")){
//				collector.collect(new Tuple2<>(word,1));
//			}
//		}
//	}


}
