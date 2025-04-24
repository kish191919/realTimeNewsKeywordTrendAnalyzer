from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkElasticsearch7Sink
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.restart_strategy import RestartStrategies

import json
import re

# --- 환경 설정 ---
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.enable_checkpointing(5000)
env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 10000))

# --- Kafka Consumer 설정 ---
kafka_props = {
    'bootstrap.servers': 'ec2-13-219-225-137.compute-1.amazonaws.com:9092',
    'group.id': 'flink-news-group',
    'auto.offset.reset': 'earliest'
}

consumer = FlinkKafkaConsumer(
    topics='news',
    deserialization_schema=SimpleStringSchema(),
    properties=kafka_props
)

# --- 데이터 처리 함수 ---
def extract_keywords(news_json):
    try:
        news_data = json.loads(news_json)
        title = news_data.get('title', '').lower()
        # 특수문자 제거 + 단어 분리
        words = re.findall(r'\b[a-z]{4,}\b', title)
        return [(word, 1) for word in words]
    except:
        return []

# --- 스트림 파이프라인 구성 ---
ds = env.add_source(consumer)

# 뉴스에서 단어 추출 후 (word, 1) 형태로 분리
words = ds.flat_map(
    lambda line: extract_keywords(line),
    output_type=Types.TUPLE([Types.STRING(), Types.INT()])
)

# 키워드 카운팅
word_counts = words \
    .key_by(lambda x: x[0]) \
    .sum(1)

# --- Elasticsearch Sink 설정 ---
def to_elasticsearch_record(word_count):
    return {
        "_index": "news_keywords",
        "_type": "_doc",
        "_source": {
            "keyword": word_count[0],
            "count": word_count[1]
        }
    }

es_hosts = [{'host': 'localhost', 'port': 9200}]

word_counts.add_sink(
    FlinkElasticsearch7Sink(
        hosts=es_hosts,
        bulk_flush_max_actions=1,
        serialization_schema=lambda x: to_elasticsearch_record(x)
    )
)

# --- 실행 ---
env.execute("Real-Time News Keyword Trend Analyzer")

