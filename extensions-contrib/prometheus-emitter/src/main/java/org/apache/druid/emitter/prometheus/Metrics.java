package org.apache.druid.emitter.prometheus;

import java.util.Map;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.prometheus.client.SimpleCollector;

public enum Metrics {
    //TODO: namespace should come from configuration
    QUERY_TIME(
            "query/time",
            new String[] {"dataSource", "type"},
            new Histogram.Builder()
                    .namespace("druid")
                    .name("query_time")
                    .labelNames("dataSource", "type")
                    .buckets(new double[] { .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300})
                    .register()),
    QUERY_BYTES(
            "query/bytes",
            new String[] {"dataSource", "type"},
            new Counter.Builder()
                    .namespace("druid")
                    .name("query_bytes_total")
                    .labelNames("dataSource", "type")
                    .register()),
    QUERY_NODE_TIME(
            "query/node/time",
            new String[] {"server"},
            new Histogram.Builder()
                    .namespace("druid")
                    .name("query_node_time")
                    .labelNames("server")
                    .buckets(new double[] { .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300})
                    .register()
    )

    ;

    private static Map<String, Metrics> map;
    static {
        for (Metrics m : Metrics.values()) {
            map.put(m.name, m);
        }
    }

    public static Metrics getByName(String name) {
        return map.get(name);
    }

    private final String name;
    private final String[] dimensions;
    private final SimpleCollector collector;

    Metrics(String name, String[] dimensions, SimpleCollector collector) {
        this.name = name;
        this.dimensions = dimensions;
        this.collector = collector;
    }

    public String getName() {
        return name;
    }

    public String[] getDimensions() {
        return dimensions;
    }

    public SimpleCollector getCollector() {
        return collector;
    }
}
