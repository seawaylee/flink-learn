package stream.cases.metric.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class MetricEvent {


    private String name;


    private Long timestamp;


    private Map<String, Object> fields;


    private Map<String, String> tags;
}