# Real-Time News Keyword Trend Analyzer

This project demonstrates a real-time news keyword trend analyzer using Apache Flink, Kafka, Elasticsearch, and Kibana. The goal is to fetch the latest news from a public API, stream the data using Kafka, process and analyze the keyword trends with Flink, and visualize the results in real-time using Elasticsearch and Kibana.

## Components

### 1. Kafka Producer (news_producer.py)
This script fetches news data from a public API (NewsAPI.org) and sends it to a Kafka topic called `news`. The producer fetches news headlines periodically and streams them to Kafka.

### 2. Flink Consumer (keyword_trend_analyzer.py)
This Flink job consumes the news data from the Kafka topic and processes the headlines. It extracts keywords from the titles and performs a trend analysis to track which keywords are most popular in real-time.

### 3. Elasticsearch & Kibana
The processed data is pushed into Elasticsearch for indexing. Kibana is used for creating real-time visualizations such as keyword frequency charts and trend graphs.

## Installation

### Prerequisites
1. **Apache Kafka** - To handle the news data stream.
2. **Apache Flink** - For processing the data stream in real-time.
3. **Elasticsearch** - For storing and querying the processed data.
4. **Kibana** - For visualizing the trends.

### Setup
1. Clone this repository:
    ```bash
    git clone https://github.com/your-username/real-time-news-keyword-trend-analyzer.git
    cd real-time-news-keyword-trend-analyzer
    ```

2. Install the required Python dependencies for the Kafka producer:
    ```bash
    pip install -r requirements.txt
    ```

3. Start Kafka and Zookeeper (You can use Docker or install locally).
4. Run the Kafka producer script:
    ```bash
    python kafka-producer/news_producer.py
    ```

5. Set up Flink and run the Flink job to start processing the news data:
    ```bash
    # Flink job setup and execution (example command)
    ./bin/flink run flink-consumer/keyword_trend_analyzer.py
    ```

6. Set up Elasticsearch and Kibana (You can use Docker for easier setup):
    - Start Elasticsearch and Kibana.
    - Use the provided setup scripts to configure them for use with this project.

7. Visualize keyword trends in Kibana using the pre-configured dashboards.

## Usage

Once the system is up and running, the Kafka producer will stream news data to Flink, where it will be processed and stored in Elasticsearch. Kibana will visualize the keyword trends as they evolve in real-time.

You can customize the project by modifying the following:
- `news_producer.py`: Change the API or modify how data is fetched.
- `keyword_trend_analyzer.py`: Adjust the Flink job to analyze different data.
- Kibana Dashboards: Customize visualizations to suit your needs.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

