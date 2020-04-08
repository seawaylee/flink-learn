package etl.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 获取原始输入数据（一般来说source都是kafka，为了简单则使用文件系统测试）
 *
 * @author SeawayLee
 * @create 2020-03-30 18:20
 */
public class FileSource implements SourceFunction<String> {
    private String filePath;
    private BufferedReader reader;
    private Random random = new Random();

    public FileSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
        String line;
        while ((line = reader.readLine()) != null) {
            TimeUnit.MILLISECONDS.sleep(random.nextInt(500));
            sourceContext.collect(line);
        }
    }

    @Override
    public void cancel() {
        if (reader == null) {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
