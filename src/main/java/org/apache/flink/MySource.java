package org.apache.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MySource implements SourceFunction<String> {
    private long count = 1L;
    private boolean isRunning = true;
    // run，开始执行，while循环，持续执行数据，
    @Override
    public void run(SourceContext<String> context) throws Exception {
        while (isRunning) {
            context.collect("消息"+ count);
            count+=1;
            Thread.sleep(1000);
        }

    }
    // cancel，取消执行
    @Override
    public void cancel() {
        isRunning = false;
    }
}























