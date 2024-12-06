package org.apache.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;

public class StateProcessorAPIReadDemo {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 指定保存点，输入保存点地址
        ExistingSavepoint savepoint = Savepoint.load(env, "E:\\大数据实验\\TestSavepoint\\savepoint-1",
                new MemoryStateBackend());
        // 三种保存状态的后端类型选择：MemoryStateBackend: 将状态保存在内存中，适用于小规模，临时的数据
        // FsStateBackend 将状态保存在文件系统中
        // RocksDBStateBackend, 将状态保存在本地或远程的RockDB数据库中
        // 从保存点获取数据（状态）                                     状态uid， 状态唯一名称， 状态类型信息
        DataSet<Integer> listState = savepoint.readListState("uid1", "state1", Types.INT);
        listState.print();
    }
}
