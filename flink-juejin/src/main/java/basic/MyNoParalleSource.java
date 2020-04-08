package basic;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 功能：每秒产生一条数据
 */
public class MyNoParalleSource implements SourceFunction<Long> {
    private long number = 1L;
    private boolean isRunning = true;
    @Override
    public void run(SourceContext<Long> sct) throws Exception {
        while (isRunning){
         sct.collect(number);
         number++;
         //每秒生成一条数据
         Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        isRunning=false;
    }
}