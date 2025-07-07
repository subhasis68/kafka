package com.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaToIcebergJob {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setString("state.backend", "rocksdb");
        conf.setString("state.checkpoints.dir", "s3a://flink-checkpoints/job-checkpoints/");

        conf.setString("fs.s3a.access.key", "minioadmin");
        conf.setString("fs.s3a.secret.key", "minioadmin");
        conf.setString("fs.s3a.endpoint", "http://minio:9000");
        conf.setString("fs.s3a.path.style.access", "true");
        conf.setString("fs.s3a.connection.ssl.enabled", "false");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        env.enableCheckpointing(10000);

        tableEnv.getConfig().addConfiguration(conf);

        // Create Iceberg catalog
        tableEnv.executeSql(
                "CREATE CATALOG iceberg WITH (\n" +
                        " 'type' = 'iceberg',\n" +
                        " 'catalog-type' = 'hive',\n" +
                        " 'uri' = 'thrift://hive-metastore:9083',\n" +
                        " 'warehouse' = 's3a://warehouse/',\n" +
                        " 'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',\n" +
                        " 's3.endpoint' = 'http://minio:9000',\n" +
                        " 's3.path-style-access' = 'true',\n" +
                        " 's3.access-key' = 'minioadmin',\n" +
                        " 's3.secret-key' = 'minioadmin'\n" +
                        ")");
        tableEnv.useCatalog("iceberg");
        tableEnv.useDatabase("default");

        // Register Kafka table inside the Iceberg catalog (works fine)
        // Register Kafka source table in default catalog
        tableEnv.useCatalog("default_catalog");
        tableEnv.useDatabase("default_database");
        System.out.println("==> Creating Kafka table...");
        tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS kafka_vehicle_data (\n" +
                        "  vehicle_id INT,\n" +
                        "  lat DOUBLE,\n" +
                        "  `long` DOUBLE,\n" +
                        "  speed DOUBLE,\n" +
                        "  temperature DOUBLE,\n" +
                        "  humidity INT,\n" +
                        "  created_at TIMESTAMP(3),\n" +
                        "  WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'test-topic',\n" +
                        "  'properties.bootstrap.servers' = 'bitnami_kafka:29092',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json',\n" +
                        "  'json.timestamp-format.standard' = 'ISO-8601'\n" +
                        ")");

        // Create Iceberg table
        tableEnv.useCatalog("iceberg");
        tableEnv.useDatabase("default");
        System.out.println("==> Creating Iceberg table...");
        tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS vehicle_data_iceberg (\n" +
                        "  vehicle_id INT,\n" +
                        "  lat DOUBLE,\n" +
                        "  `long` DOUBLE,\n" +
                        "  speed DOUBLE,\n" +
                        "  temperature DOUBLE,\n" +
                        "  humidity INT,\n" +
                        "  created_at TIMESTAMP(3)\n" +
                        ") PARTITIONED BY (vehicle_id)");

        // Optional debug: print Kafka source (confirms ingestion)
        // tableEnv.useCatalog("default_catalog");
        // tableEnv.useDatabase("default_database");
        // TableResult debugResult = tableEnv
        // .executeSql("SELECT * FROM kafka_vehicle_data");
        // debugResult.print(); // remove after verifying Kafka source is non-empty

        // Insert from Kafka to Iceberg (continuous job)
        tableEnv.useCatalog("iceberg");
        tableEnv.useDatabase("default");
        System.out.println("==> Running INSERT INTO ...");

        try {
            tableEnv.executeSql(
                    "INSERT INTO vehicle_data_iceberg\n" +
                            "SELECT * FROM default_catalog.default_database.kafka_vehicle_data");
            System.out.println("[Insert] started successfully.");
        } catch (Exception e) {
            System.err.println("[Insert] failed: " + e.getMessage());
            e.printStackTrace();
        }

    }
}
