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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;



import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

//import static org.apache.flink.table.api.Expressions.$;
//静态导入方法,可将expressions.$简写为$

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))

 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//---------------------------------timeWindows add----------------------------------------
		DataStream<Tuple2<String, Integer>> dataStream = env.addSource(new MySource())
				.flatMap(new Splitter())
				.keyBy(0)
//				.timeWindow(Time.seconds(10))
				.sum(1);
		dataStream.print();
		//打印执行计划的JSON
		System.out.println(env.getExecutionPlan());

		//------------------------------------end-------------------------------------------

//		EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
//				.useBlinkPlanner()
//				.inStreamingMode()
//				.build();
//
//		//创造表环境
//		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,bsSettings);
//		//用流环境创造数据
//		DataStream<String> dataStream = env.addSource(new MySource());
//		//再将数据变为表
//		Table table_inter = tEnv.fromDataStream(dataStream, Expressions.$("word"));
//		//模糊查找找到其中有t的
//		Table table = table_inter.where(Expressions.$("word").like("%t%"));
//		String explanation_old = tEnv.explain(table);
//		System.out.println(explanation_old);
//		// 将table转换为对应的dataStream
//		tEnv.toAppendStream(table, Row.class)
//				.print("table");



		













		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

	//------------------MySource 流式数据创造，每隔一秒一次-----------------------
	public static class  MySource implements SourceFunction<String> {
		private long count = 1;
		private boolean isRunning = true;
		@Override
		public void run(SourceContext<String> souceLne) throws Exception {
			while (isRunning){
				List<String> stringList = new ArrayList<>();
				stringList.add("world");
				stringList.add("Flink");
				stringList.add("Stream");
				stringList.add("Batch");
				stringList.add("Table");
				stringList.add("SQL");
				stringList.add("hello");
				int size = stringList.size();
				int i = new Random().nextInt(size);
				souceLne.collect(stringList.get(i));
				Thread.sleep(1000);
			}
		}
		@Override
		public void cancel(){
			isRunning = false;
		}
	}

	//--------------------------end--------------------------------------

	//--------------------------自定义flagMap,拆分sentence，用collector收集---------------------------
	public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception{
			for (String word:sentence.split(" ")){
				out.collect(new Tuple2<String, Integer>(word, 1));
			}
		}
	}

	//-------------------------end-------------------------------




}
