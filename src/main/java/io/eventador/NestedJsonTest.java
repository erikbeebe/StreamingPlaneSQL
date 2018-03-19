package io.eventador;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.Kafka011JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import org.apache.flink.table.descriptors.Schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

//import org.apache.flink.streaming.util.serialization.JsonRowSerializationSchema;
//import com.google.gson.GsonBuilder;

public class NestedJsonTest {
        public static void main(String[] args) throws Exception {
            // Read parameters from command line
            final ParameterTool params = ParameterTool.fromArgs(args);

            if(params.getNumberOfParameters() < 4) {
                System.out.println("\nUsage: FlinkReadKafka --read-topic <topic> --write-topic <topic> --bootstrap.servers <kafka brokers> --group.id <groupid>");
                return;
            }

            Properties kparams = params.getProperties();
            kparams.setProperty("auto.offset.reset", "earliest");
            kparams.setProperty("flink.starting-position", "earliest");
            kparams.setProperty("group.id", UUID.randomUUID().toString());

            // setup streaming environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
            env.enableCheckpointing(300000); // 300 seconds
            env.getConfig().setGlobalJobParameters(params);
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            // Get datastream from Kafka
            DataStream<ObjectNode> inputStream = env.addSource(
                    new FlinkKafkaConsumer011<>(params.getRequired("read-topic"), new JSONDeserializationSchema(), kparams))
                    .name("Kafka 0.11 Source");

            // POJOize
            DataStream<JsonModel> jsonModel = inputStream
                    .keyBy(jsonNode -> jsonNode.get("field1").textValue())
                    .map(new JsonMapper())
                    .name("Timestamp -> KeyBy ICAO -> Map");

            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

            //Table table = tableEnv.fromDataStream(jsonModel, "field1, field2, field3nested1, mytimestamp.rowtime");
            tableEnv.registerDataStream("foobar", jsonModel, "field1, field2, field3nested1, mytimestamp.rowtime");


            String sql = "SELECT count(*) AS mycount, field1, "
                    + "HOP_START(mytimestamp, INTERVAL '5' SECOND, INTERVAL '1' HOUR), "
                    + "HOP_END(mytimestamp, INTERVAL '5' SECOND, INTERVAL '1' HOUR) "
                    + "FROM foobar "
                    + "GROUP BY HOP(mytimestamp, INTERVAL '5' SECOND, INTERVAL '1' HOUR), field1";

            //String sql = "SELECT * from foobar";

            Table flight_table = tableEnv.sql(sql);
            flight_table.printSchema();

            // stdout debug stream, prints raw datastream to logs
            //DataStream<Tuple2<Boolean, Row>> planeRow = tableEnv.toRetractStream(flight_table, Row.class);
            DataStream<Row> planeRow = tableEnv.toAppendStream(flight_table, Row.class);
            planeRow.print();

            env.execute("StreamingPlaneSQL");
        }

        // Simple JSONifer
        private static class PlaneSchema implements SerializationSchema<Tuple2<String, Long>> {
            @Override
            public byte[] serialize(Tuple2<String, Long> tuple2) {
                Gson payload = new Gson();
                Airplane airplane = new Airplane();

                airplane.icao = tuple2.f0.toString();
                airplane.count = tuple2.f1.toString();

                return payload.toJson(airplane).getBytes();
            }
        }

    private static class JsonMapper implements MapFunction<ObjectNode, JsonModel> {
            @Override
            public JsonModel map(ObjectNode jsonFoo) {
                JsonModel js = new JsonModel();
                js.field1 = jsonFoo.get("field1").asText();
                js.field2 = jsonFoo.get("field2").asText();
                js.field3nested1 = jsonFoo.get("field3").get("nested1").asText();

                return js;
            }
    }

    /*
    class TimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<ObjectNode> {
        public TimestampExtractor() {
            super(Time.seconds(30000L));
        }

        @Override
        public long extractTimestamp(Long element, long previousElementTimestamp) {
            return previousElementTimestamp;
        }
    }
    */

    private static class GhettoPlaneSchema implements SerializationSchema<Row> {
        @Override
        public byte[] serialize(Row myRow) {
            return myRow.toString().getBytes();
        }
    }

        // Container class for data model
        private static class Airplane {
            private String icao;
            private String count;

            Airplane() {
                // no arg constructor
            }
        }
}

