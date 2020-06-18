package stream.broadcast;

import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stream.cases.metric.dto.MetricEvent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author SeawayLee
 * @create 2020-06-15 10:56
 */
public class TestBroadcastStreamMain {

    PreparedStatement ps;
    private Connection connection;
    private volatile boolean isRunning = true;
    private static final Logger LOG = LoggerFactory.getLogger(TestBroadcastStreamMain.class);

    public static void main(String[] args) {


    }

    public class MyBroadcastProcessFunction extends BroadcastProcessFunction<MetricEvent, Map<String, String>, MetricEvent> {
        private MapStateDescriptor<String, String> alarmRulesMapStateDescriptor;

        public MyBroadcastProcessFunction(MapStateDescriptor<String, String> alarmRulesMapStateDescriptor) {
            this.alarmRulesMapStateDescriptor = alarmRulesMapStateDescriptor;
        }

        @Override
        public void processElement(MetricEvent metricEvent, ReadOnlyContext readOnlyContext, Collector<MetricEvent> collector) throws Exception {
            ReadOnlyBroadcastState<String, String> broadcastState = readOnlyContext.getBroadcastState(alarmRulesMapStateDescriptor);
            Map<String, String> tags = metricEvent.getTags();
            if (!tags.containsKey("type") && !tags.containsKey("type_id")) {
                return;
            }
            String targetId = broadcastState.get(tags.get("type")) + tags.get("type_id");
            if (!StringUtils.isEmpty(targetId)) {
                metricEvent.getTags().put("target_id", targetId);
                collector.collect(metricEvent);
            }
        }

        @Override

        public void processBroadcastElement(Map<String, String> stringStringMap, Context context, Collector<MetricEvent> collector) throws Exception {

        }
    }

    /**
     * 构建Souce获取监控元数据
     */
    public class GetAlertSourceFunction extends RichSourceFunction<List<AlertRule>> {
        @Override
        public void run(SourceContext<List<AlertRule>> sourceContext) throws Exception {
            List<AlertRule> list = new ArrayList<>();
            while (isRunning) {
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    AlertRule alertRule = AlertRule.builder()
                            .id(resultSet.getInt("id"))
                            .name(resultSet.getString("name"))
                            .measurement(resultSet.getString("measurement"))
                            .thresholds(resultSet.getString("thresholds"))
                            .build();
                    list.add(alertRule);
                }
            }
            LOG.info("=======select alarm notify from mysql, size = {}, map = {}", list.size(), list);
            sourceContext.collect(list);
            list.clear();
            Thread.sleep(1000 * 60);
        }

        @Override
        public void cancel() {
            try {
                if (connection != null) {
                    connection.close();
                }
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            isRunning = false;
        }

    }


    @Getter
    @Setter
    @Builder
    @NoArgsConstructor
    private class AlertRule {
        private Integer id;
        private String name;
        private String measurement;
        private String thresholds;
    }
}


