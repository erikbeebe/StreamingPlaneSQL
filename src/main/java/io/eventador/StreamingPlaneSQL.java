package io.eventador;

import org.apache.calcite.model.JsonSchema;
import org.apache.calcite.model.ModelHandler;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.JsonRowSerializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Properties;
import java.util.UUID;

public class StreamingPlaneSQL {
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
            kparams.setProperty("group.id", UUID.randomUUID().toString());

            // setup streaming environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
            env.enableCheckpointing(300000); // 300 seconds
            env.getConfig().setGlobalJobParameters(params);
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

            KafkaTableSource kafkaTableSource = Kafka010JsonTableSource.builder()
                    .forTopic(params.getRequired("read-topic"))
                    .withKafkaProperties(kparams)
                    .withSchema(TableSchema.builder()
                            .field("icao", Types.STRING())
                            .field("flight", Types.STRING())
                            .field("timestamp_verbose", Types.STRING())
                            .field("msg_type", Types.STRING())
                            .field("track", Types.STRING())
                            .field("timestamp", Types.SQL_TIMESTAMP())
                            .field("altitude", Types.STRING())
                            .field("counter", Types.STRING())
                            .field("lon", Types.STRING())
                            .field("lat", Types.STRING())
                            .field("vr", Types.STRING())
                            .field("speed", Types.STRING())
                            .field("mytimestamp", Types.SQL_TIMESTAMP())
                            .build())
                    .withRowtimeAttribute("mytimestamp", new ExistingField("timestamp"), new BoundedOutOfOrderTimestamps(30000L))
                    .build();

            // register the table and apply sql to stream
            tableEnv.registerTableSource("flights", kafkaTableSource);

            // run some SQL to filter results where a key is not null
            String sql = "SELECT icao, count(icao) AS pings "
                    + "FROM flights "
                    + "WHERE icao IS NOT null "
                    + "GROUP BY TUMBLE(mytimestamp, INTERVAL '5' SECOND), icao";

            Table flight_table = tableEnv.sql(sql);

            // stdout debug stream, prints raw datastream to logs
            DataStream<Row> planeRow = tableEnv.toAppendStream(flight_table, Row.class);
            planeRow.print();

            // stream for feeding tumbling window count back to Kafka
            //TupleTypeInfo<Tuple2<String, Long>> planeTupleType = new TupleTypeInfo<>(Types.STRING(), Types.LONG());
            //DataStream<Tuple2<String, Long>> planeTuple = tableEnv.toAppendStream(flight_table, planeTupleType);

            // send JSON-ified stream to Kafka
            //planeTuple.addSink(new FlinkKafkaProducer010<>(
            //            params.getRequired("write-topic"),
            //            new PlaneSchema(),
            //            params.getProperties())).name("Write Planes to Kafka");

            env.execute("FlinkReadWriteKafkaJSON");
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

        // Container class for data model
        private static class Airplane {
            private String icao;
            private String count;

            Airplane() {
                // no arg constructor
            }
        }
}
