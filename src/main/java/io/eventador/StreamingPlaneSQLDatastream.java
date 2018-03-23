package io.eventador;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

//import org.apache.flink.streaming.util.serialization.JsonRowSerializationSchema;
//import com.google.gson.GsonBuilder;

public class StreamingPlaneSQLDatastream {
        public static void main(String[] args) throws Exception {
            // Read parameters from command line
            final ParameterTool params = ParameterTool.fromArgs(args);

            if(params.getNumberOfParameters() < 4) {
                System.out.println("\nUsage: FlinkReadKafka --read-topic <topic> --write-topic <topic> --bootstrap.servers <kafka brokers> --group.id <groupid>");
                return;
            }

            Properties kparams = params.getProperties();
            kparams.setProperty("auto.offset.reset", "earliest");
            //kparams.setProperty("flink.starting-position", "earliest");
            //kparams.setProperty("group.id", UUID.randomUUID().toString());

            // setup streaming environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
            env.enableCheckpointing(300000); // 300 seconds
            env.getConfig().setGlobalJobParameters(params);
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            env.setParallelism(1);
            env.getConfig().setAutoWatermarkInterval(1000L);

            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

            // Get datastream from Kafka
            DataStream<ObjectNode> inputStream = env.addSource(
                    new FlinkKafkaConsumer011<>(params.getRequired("read-topic"), new JSONDeserializationSchema(), kparams)
                    .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<ObjectNode>() {
                        @Nullable
                        @Override
                        public Watermark checkAndGetNextWatermark(ObjectNode lastElement, long extractedTimestamp) {
                            return new Watermark(extractedTimestamp);
                        }

                        @Override
                        public long extractTimestamp(ObjectNode element, long previousElementTimestamp) {
                            return previousElementTimestamp;
                        }
                    }))
                    .name("Kafka 0.11 Source");

            // POJOize
            DataStream<PlaneModel> planeModel = inputStream
                    .keyBy(jsonNode -> jsonNode.get("icao").textValue())
                    .map(new PlaneMapper())
                    .name("Timestamp -> KeyBy ICAO -> Map");

            //Table table = tableEnv.fromDataStream(jsonModel, "field1, field2, field3nested1, mytimestamp.rowtime");
            tableEnv.registerFunction("isMilitary", new MilitaryPlanes.IsMilitary());
            tableEnv.registerDataStream("planes", planeModel,
                    "icao, flight, timestamp_verbose, msg_type, track, timestamp, altitude, counter, lon, lat, speed, mytimestamp.rowtime");

            // Define a TableSink
            /*
            String[] sinkFields = {"pings", "hopStart", "hopEnd", "avgAltitude", "maxAltitute", "minAltitude"}
            TypeInformation[] sinkFieldTypes = { Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG(), Types.LONG(), Types.LONG() };
            TableSink tSink = tableEnv.registerTableSink("kafkaSink", sinkFields, sinkFieldTypes, );
            */

            String sql = "SELECT icao, count(icao) AS pings, "
                    + "HOP_START(mytimestamp, INTERVAL '5' SECOND, INTERVAL '5' MINUTE) as hopStart, "
                    + "HOP_END(mytimestamp, INTERVAL '5' SECOND, INTERVAL '5' MINUTE) hopEnd, "
                    + "AVG(altitude) AS avgAltitude, "
                    + "MAX(altitude) AS maxAltitude, "
                    + "MIN(altitude) AS minAltitude, "
                    + "isMilitary(icao) as IsMilitaryPlane "
                    + "FROM planes "
                    + "WHERE icao IS NOT null "
                    + " AND altitude IS NOT null "
                    + "GROUP BY HOP(mytimestamp, INTERVAL '5' SECOND, INTERVAL '5' MINUTE), icao";

            Table flight_table = tableEnv.sql(sql);
            flight_table.writeToSink(
                    new Kafka010JsonTableSink(
                        "airplanes_json",
                        kparams));

            // stdout debug stream, prints raw datastream to logs
            DataStream<Row> planeRow = tableEnv.toAppendStream(flight_table, Row.class);
            planeRow.print();

            String jobName = String.format("StreamingPlaneSQL -> Source topic: %s", params.get("read-topic"));
            env.execute(jobName);
        }

    private static class MilitaryPlanes {
            public static Boolean isMilitary(String icao) {
                return icao.startsWith("AE");
            }

            public static class IsMilitary extends ScalarFunction {
                public boolean eval(String icao) {
                    return isMilitary(icao);
                }
            }
    }

    private static class PlaneMapper implements MapFunction<ObjectNode, PlaneModel> {
        @Override
        public PlaneModel map(ObjectNode planejson) {
            PlaneModel plane = new PlaneModel();

            // get new values from JSON, these should always be in the payload
            plane.msg_type = planejson.get("msg_type").asInt();
            plane.timestamp_verbose = planejson.get("timestamp_verbose").textValue();
            plane.timestamp = planejson.get("timestamp").asLong();
            plane.icao = planejson.get("icao").textValue();
            plane.counter = planejson.get("counter").asLong();

            switch (plane.msg_type) {
                case 1: plane.flight = planejson.get("flight").textValue();
                    break;
                case 3: plane.altitude = planejson.get("altitude").asInt();
                    plane.lat = planejson.get("lat").asDouble();
                    plane.lon = planejson.get("lon").asDouble();
                    break;
                case 4: plane.speed = planejson.get("speed").asInt();
                    break;
                case 5: plane.altitude = planejson.get("altitude").asInt();
                    break;
                case 7: plane.altitude = planejson.get("altitude").asInt();
                    break;
            }

            //System.out.println("MAPPED RECORD: " + plane.toString());
            return plane;
        }
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

    private static class PlainPlaneSchema implements SerializationSchema<Row> {
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
