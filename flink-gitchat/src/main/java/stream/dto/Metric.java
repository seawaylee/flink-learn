package stream.dto;

import java.util.Map;

/**
 * @author SeawayLee
 * @create 2020-04-16 16:19
 */
public class Metric {
    public String name;
    public long tsp;
    public Map<String, Object> fields;
    public Map<String, String> tags;

    public Metric() {
    }

    public Metric(String name, long tsp, Map<String, Object> fields, Map<String, String> tags) {
        this.name = name;
        this.tsp = tsp;
        this.fields = fields;
        this.tags = tags;
    }

    public String getName() {
        return name;
    }

    public Metric setName(String name) {
        this.name = name;
        return this;
    }

    public long getTsp() {
        return tsp;
    }

    public Metric setTsp(long tsp) {
        this.tsp = tsp;
        return this;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public Metric setFields(Map<String, Object> fields) {
        this.fields = fields;
        return this;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public Metric setTags(Map<String, String> tags) {
        this.tags = tags;
        return this;
    }
}
