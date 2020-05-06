package cep;

import cep.dto.Event;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * @author SeawayLee
 * @create 2020-05-06 16:53
 */
public class CepTestMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Source
        SingleOutputStreamOperator<Event> eventDataStream = env.socketTextStream("127.0.0.1", 9200)
                .flatMap(new FlatMapFunction<String, Event>() {
                    @Override
                    public void flatMap(String line, Collector<Event> collector) throws Exception {
                        if (!StringUtils.isEmpty(line)) {
                            String[] lineArr = line.split(",");
                            if (lineArr.length == 2) {
                                collector.collect(new Event(Integer.valueOf(lineArr[0]), lineArr[1]));
                            }
                        }
                    }
                });
        // CEP
        Pattern<Event, ?> pattern = Pattern
                .<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                           @Override
                           public boolean filter(Event event) throws Exception {
                               System.out.println("start " + event.getId());
                               return event.getId() == 42;
                           }
                       }
                )
                .next("middle")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        System.out.println("middle " + event.getId());
                        return event.getId() >= 10;
                    }
                });
        CEP.pattern(eventDataStream, pattern).select(new PatternSelectFunction<Event, String>() {
            @Override
            public String select(Map<String, List<Event>> p) throws Exception {
                System.out.println("p = " + p);
                String startStr = new StringJoiner(",").add(p.get("start").get(0).getId().toString()).add(p.get("start").get(0).getName()).toString();
                String midStr = new StringJoiner(",").add(p.get("middle").get(0).getId().toString()).add(p.get("middle").get(0).getName()).toString();
                return startStr + " && " + midStr;
            }
        }).print();
        env.execute("test cep");
    }
}
