package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.Properties;

public class KafkaToTimescaleDBJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        inputStream.map(new TimescaleDBWriter());

        env.execute("Flink Kafka to TimescaleDB Job");
    }

    public static class TimescaleDBWriter implements MapFunction<String, Void> {
        private static final ObjectMapper mapper = new ObjectMapper();
        private static final String JDBC_URL = "jdbc:postgresql://timescaledb:5432/kafkadb";
        private static final String JDBC_USER = "postgres";
        private static final String JDBC_PASSWORD = "mysecretpassword";

        @Override
        public Void map(String value) throws Exception {
            Class.forName("org.postgresql.Driver"); // Force loading the driver
            JsonNode json = mapper.readTree(value);

            int vehicleId = json.get("vehicle_id").asInt();
            float lat = (float) json.get("lat").asDouble();
            float lon = (float) json.get("long").asDouble();
            float speed = (float) json.get("speed").asDouble();
            float temp = (float) json.get("temperature").asDouble();
            int hum = json.get("humidity").asInt();
            Instant timestamp = Instant.now();

            try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)) {
                String insertSQL = "INSERT INTO vehicle_data (vehicle_id, lat, long, speed, temperature, humidity, created_at) "
                        +
                        "VALUES (?, ?, ?, ?, ?, ?, ?)";
                try (PreparedStatement stmt = conn.prepareStatement(insertSQL)) {
                    stmt.setInt(1, vehicleId);
                    stmt.setFloat(2, lat);
                    stmt.setFloat(3, lon);
                    stmt.setFloat(4, speed);
                    stmt.setFloat(5, temp);
                    stmt.setInt(6, hum);
                    stmt.setTimestamp(7, java.sql.Timestamp.from(timestamp));
                    stmt.executeUpdate();
                }
            }

            return null;
        }
    }
}
