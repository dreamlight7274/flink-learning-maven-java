package org.apache.flink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.StateBootstrapFunction;


public class StateProcessorAPIWriteDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> input = env.fromElements(1, 2, 3, 4, 5, 6);
        //---------------------------------------------
        BootstrapTransformation transformation = OperatorTransformation
                .bootstrapWith(input)
                .transform(new MySimpleBootstrapFunction());
        //---------------------------------------------编写输入算子状态的方法，通过这种方法预定的数据能被添加到状态中
        int maxParallelism = 128;
        Savepoint.create(new MemoryStateBackend(), maxParallelism)
                .withOperator("uid1", transformation)
                .write("E:\\大数据实验\\TestSavepoint\\savepoint-1");
        env.execute();
// 元数据，描述数据的数据，比如文件的创建日期，修改日期，大小等信息

    }

    private static class MySimpleBootstrapFunction extends StateBootstrapFunction<Integer> {
        private ListState<Integer> state;

        // 将给定值写入算子状态
        @Override
        public void processElement(Integer value, Context context) throws Exception{
            state.add(value);
        }
        // 处理状态的快照
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception{

        }

        // 创建函数实例，初始化状态时将使用此方法
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception{
            state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("state1", Types.INT));
        }
    }
}
