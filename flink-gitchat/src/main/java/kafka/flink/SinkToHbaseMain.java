package kafka.flink;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

import static kafka.constant.HBaseConstant.*;

/**
 * @author SeawayLee
 * @create 2020-04-21 18:18
 */
public class SinkToHbaseMain {
    public static final String HBASE_TABLE_NAME = "user";

    static final byte[] COL_FAMILY = "col_fam1".getBytes(ConfigConstants.DEFAULT_CHARSET);

    static final byte[] COL_NAME = "name".getBytes(ConfigConstants.DEFAULT_CHARSET);

    private static final String[] WORDS = new String[]{
            "To be, or not to be,--that is the question:--",
            "Whether 'tis nobler in the mind to suffer"
    };

    public static void main(String[] args) throws Exception {
        streamReadAndSink();
    }

    /**
     * 批处理-写数据
     */
    public static void put() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Job job = Job.getInstance();
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, HBASE_TABLE_NAME);
        job.getConfiguration().set("mapred.output.dir", "/tmp");

        env.fromElements(WORDS)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] words = line.split("\\W+");
                        for (String word : words) {
                            collector.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .groupBy(0)
                .sum(1)
                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<Text, Mutation>>() {
                    private transient Tuple2<Text, Mutation> reuse;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        reuse = new Tuple2<>();
                    }

                    @Override
                    public Tuple2<Text, Mutation> map(Tuple2<String, Integer> value) throws Exception {
                        reuse.f0 = new Text(value.f0);
                        Put put = new Put(value.f0.getBytes(ConfigConstants.DEFAULT_CHARSET));
                        put.addColumn(COL_FAMILY, COL_NAME, Bytes.toBytes(value.f1.toString()));
                        reuse.f1 = put;
                        return reuse;
                    }
                })
                .output(new HadoopOutputFormat<>(new TableOutputFormat<>(), job));
        env.execute("Flink Connector HBase sink Example");
    }

    /**
     * 批处理-读数据
     */
    public static void scan() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.createInput(new TableInputFormat<Tuple2<String, String>>() {
            private Tuple2<String, String> reuse = new Tuple2<>();

            @Override
            protected Scan getScanner() {
                Scan scan = new Scan();
                scan.addColumn(COL_FAMILY, COL_NAME);
                return scan;
            }

            @Override
            protected String getTableName() {
                return HBASE_TABLE_NAME;
            }

            @Override
            protected Tuple2<String, String> mapResultToTuple(Result result) {
                String key = Bytes.toString(result.getRow());
                String val = Bytes.toString(result.getValue(COL_FAMILY, COL_NAME));
                reuse.setField(key, 0);
                reuse.setField(val, 1);
                return reuse;
            }
        }).filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value) throws Exception {
                return value.f1.startsWith("SHIT");
            }
        }).print();
        env.execute("Sink to habse");
    }

    /**
     * 流处理-读写数据
     */
    public static void streamReadAndSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.addSource(new SourceFunction<String>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (isRunning) {
                    sourceContext.collect(String.valueOf(Math.floor(Math.random() * 100)));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        dataStream.writeUsingOutputFormat(new HbaseOutputFromat());
        env.execute("Flink HBase connector flink streaming.");

    }

    private static class HbaseOutputFromat implements OutputFormat<String> {
        private org.apache.hadoop.conf.Configuration configuration;
        private Connection connection = null;
        private Table table = null;
        private int rowNumber = 0;

        @Override
        public void configure(Configuration params) {
            configuration = HBaseConfiguration.create();
            configuration.set(HBASE_ZOOKEEPER_QUORUM, "localhost:2181");
            configuration.set(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, "2081");
            configuration.set(HBASE_RPC_TIMEOUT, "30000");
            configuration.set(HBASE_CLIENT_OPERATION_TIMEOUT, "30000");
            configuration.set(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "30000");
        }

        @Override
        public void open(int taskNum, int numTasks) throws IOException {
            String tableNameStr = "random_nums";
            connection = ConnectionFactory.createConnection(configuration);
            TableName tableName = TableName.valueOf(tableNameStr);
            Admin admin = connection.getAdmin();
            if (!admin.tableExists(tableName)) {
                System.out.println("=========== 表不存在 ============" + tableNameStr);
                admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor("info")));
            }
            table = connection.getTable(tableName);
        }

        @Override
        public void writeRecord(String s) throws IOException {
            Put put = new Put(Bytes.toBytes(String.valueOf(rowNumber)));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("val"), Bytes.toBytes(s));
            rowNumber++;
            table.put(put);
        }

        @Override
        public void close() throws IOException {
            table.close();
            connection.close();
        }
    }


}
