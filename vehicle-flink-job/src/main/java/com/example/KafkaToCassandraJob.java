package com.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Properties;

public class KafkaToCassandraJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "bitnami_kafka:29092");
        kafkaProps.setProperty("group.id", "vehicle-group");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("bitnami_kafka:29092")
                .setTopics("test-topic")
                .setGroupId("vehicle-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> inputStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");

        inputStream.map(new CassandraWriter());

        env.execute("Flink Kafka to Cassandra Job");
    }

    public static class CassandraWriter implements MapFunction<String, Void> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public Void map(String value) throws Exception {
            JsonNode json = mapper.readTree(value);

            int vehicleId = json.get("vehicle_id").asInt();
            float lat = (float) json.get("lat").asDouble();
            float lon = (float) json.get("long").asDouble();
            float speed = (float) json.get("speed").asDouble();
            float temp = (float) json.get("temperature").asDouble();
            int hum = json.get("humidity").asInt();

            try (CqlSession session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress("cassandra-node", 9042))
                    .withLocalDatacenter("datacenter1")
                    .withKeyspace("kafkadb")
                    .build()) {
                session.execute(
                        "INSERT INTO vehicle_data (vehicle_id, lat, long, speed, temperature, humidity, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        vehicleId, lat, lon, speed, temp, hum, Instant.now());
            }

            return null;
        }
    }
}