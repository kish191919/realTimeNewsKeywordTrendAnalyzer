package org.danny.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.regex.*;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.elasticsearch.action.index.IndexRequest;

public class NewsKeywordTrendJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka Consumer 설정
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-news-group");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("news", new org.apache.flink.api.common.serialization.SimpleStringSchema(), properties);
        DataStream<String> newsStream = env.addSource(consumer);

        // 단어 추출 및 카운트
        DataStream<Tuple2<String, Integer>> wordCounts = newsStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        try {
                            String title = value.toLowerCase();
                            Matcher matcher = Pattern.compile("\\b[a-z]{4,}\\b").matcher(title);
                            while (matcher.find()) {
                                out.collect(new Tuple2<>(matcher.group(), 1));
                            }
                        } catch (Exception e) {
                            // 무시
                        }
                    }
                })
                .keyBy(value -> value.f0)
                .sum(1);

        // Elasticsearch Sink 설정
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200, "http"));

        ElasticsearchSink.Builder<Tuple2<String, Integer>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                (Tuple2<String, Integer> element, RuntimeContext ctx, RequestIndexer indexer) -> {
                    Map<String, Object> json = new HashMap<>();
                    json.put("keyword", element.f0);
                    json.put("count", element.f1);

                    IndexRequest request = Requests.indexRequest()
                            .index("news_keywords")
                            .source(json);

                    indexer.add(request);
                }
        );

        wordCounts.addSink(esSinkBuilder.build());

        env.execute("Flink Java News Keyword Analyzer");
    }
}
