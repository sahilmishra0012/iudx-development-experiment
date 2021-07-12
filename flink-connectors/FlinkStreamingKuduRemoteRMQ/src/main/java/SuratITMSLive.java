import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.connectors.kudu.streaming.KuduSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.types.Row;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.kudu.client.RowError;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.connectors.kudu.connector.failure.KuduFailureHandler;

import javax.xml.bind.DatatypeConverter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class SuratITMSLive {

    private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "127.0.0.1:7051");

    public static void main(String[] args) throws Exception {

        // creating environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	env.enableCheckpointing(1000);
        // Creating connection configuration for RMQ Source
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("databroker.iudx.org.in")
                .setUserName("datakaveri.org/d4e8037cbd0028259e38421ddc92f97d3042e725")
                .setPassword("OGID7Q5AH8oYIWAe")
                .setPort(24567)
                .setVirtualHost("IUDX")
                .setUri("amqps://datakaveri.org%2Fd4e8037cbd0028259e38421ddc92f97d3042e725:OGID7Q5AH8oYIWAe@databroker.iudx.org.in:24567/IUDX")
                .build();

        // Fetching input data flowing from RQM through the source stream.
        final DataStream<Row> stream = env
                .addSource(new CustomRMQSource(
                        connectionConfig,
                        "datakaveri.org/d4e8037cbd0028259e38421ddc92f97d3042e725/Surat_ITMS_Subscriber",
                        false,
                        new CustomDeserializationSchema()
                ))
                .setParallelism(1);


        stream.keyBy((JSONObject msg) -> msg.getString("primary_key"))
                .process(new MyProcessFunction())
                .addSink(sink);



        public class MyProcessFunction
                extends KeyedProcessFunction<JSONObject,Row> {

            /* Something temporary for now */
            private String STATE_NAME = "api state";

            // Trip ID is our state
            private ValueState<JSONObject> streamState;


            public MyProcessFunction() {
            }

            @Override
            public void open(Configuration config) {
                ValueStateDescriptor<JSONObject> stateDescriptor =
                        new ValueStateDescriptor<>(STATE_NAME, JSONObject.class);
                streamState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(JSONObject msg,
                                       Context context, Collector<Row> out) throws Exception {
                Message previousMessage = streamState.value();
                /* Update state with current message if not done */
                if (previousMessage == null) {
                    streamState.update(msg);
                } else {
                }
                try {
                    // Do ROW construction stuff here
                    out.collect(transformedMessage);
                }
            } catch (Exception e) {
            }
    streamState.update(msg);
        }















<<<<<<< HEAD:flink-connectors/FlinkStreamingKuduRemoteRMQ/src/main/java/SuratITMSLive.java














        String tableName = "surat_itms_live334";
=======
        String tableName = "surat_itms_data_june21";
>>>>>>> ade4b3b8d35f76953d1ca6ad8123d70762262002:flink-connectors/FlinkStreamingKuduRemoteRMQ/src/main/java/RMQKuduSink.java
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();

        try {
            createExampleTable(client, tableName);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Table already exist or has creation problem.");
        }

        // kudu sink
<<<<<<< HEAD:flink-connectors/FlinkStreamingKuduRemoteRMQ/src/main/java/SuratITMSLive.java
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters("127.0.0.1:7051").build();
=======
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters("kudu_kudu-master-1_1:7051").build();
>>>>>>> ade4b3b8d35f76953d1ca6ad8123d70762262002:flink-connectors/FlinkStreamingKuduRemoteRMQ/src/main/java/RMQKuduSink.java
        try{
            KuduSink<Row> sink = new KuduSink<>(
                    writerConfig,
                    KuduTableInfo.forTable(tableName),
                    new RowOperationMapper(
                            new String[]{"primary_key","trip_id","id","route_id","trip_direction","actual_trip_start_time","last_stop_arrival_time","vehicle_label","license_plate","last_stop_id","speed","observationDateTime","trip_delay","location_type","coordinate0","coordinate1"},
                            AbstractSingleOperationMapper.KuduOperation.INSERT), new CustomKuduFailureHandler());
            stream.addSink(sink);
        }
        catch(Exception e){
            System.out.println("Duplicate Row");
            System.out.println(e);
        }



        env.execute("surat_itms_live");

    }

    static void createExampleTable(KuduClient client, String tableName) throws KuduException {

        List<ColumnSchema> columns = new ArrayList<>(16);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("primary_key", Type.STRING)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_id", Type.STRING)
                .nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("route_id", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_direction", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("actual_trip_start_time", Type.UNIXTIME_MICROS).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("last_stop_arrival_time", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("vehicle_label", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("license_plate", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("last_stop_id", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("speed", Type.DOUBLE).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("observationDateTime", Type.UNIXTIME_MICROS).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_delay", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("location_type", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("coordinate0", Type.DOUBLE).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("coordinate1", Type.DOUBLE).nullable(true)
                .build());
        Schema schema = new Schema(columns);

        CreateTableOptions cto = new CreateTableOptions();
        cto.setNumReplicas(1);
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("primary_key");
        int numBuckets = 8;
        cto.addHashPartitions(hashKeys, numBuckets);

        // Create the table.
        client.createTable(tableName, schema, cto);
        System.out.println("Created table " + tableName);
    }


    static class CustomDeserializationSchema extends AbstractDeserializationSchema<Row> {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        @Override
        public Row deserialize(byte[] bytes) {

            df.setTimeZone(TimeZone.getTimeZone("UTC"));

            String streamData = new String(bytes);
            Row values = new Row(16);
            JSONObject obj = new JSONObject(streamData);
<<<<<<< HEAD:flink-connectors/FlinkStreamingKuduRemoteRMQ/src/main/java/SuratITMSLive.java
=======
            JSONObject location = obj.getJSONObject("location");
            JSONArray coordinates = location.getJSONArray("coordinates");
            String actualTripStartTime =  obj.get("actual_trip_start_time").toString();
            String observationDateTime =  obj.get("observationDateTime").toString();

            String keyString=obj.get("trip_id")+obj.get("observationDateTime").toString();
            String primaryKey = UUID.nameUUIDFromBytes(keyString.getBytes()).toString();
>>>>>>> ade4b3b8d35f76953d1ca6ad8123d70762262002:flink-connectors/FlinkStreamingKuduRemoteRMQ/src/main/java/RMQKuduSink.java

            try{
                JSONObject location = obj.getJSONObject("location");
                JSONArray coordinates = location.getJSONArray("coordinates");
                String actualTripStartTime =  obj.get("actual_trip_start_time").toString();
                String observationDateTime =  obj.get("observationDateTime").toString();

                String keyString=obj.get("trip_id")+obj.get("observationDateTime").toString();
                String primaryKey = UUID.nameUUIDFromBytes(keyString.getBytes()).toString();
                if (primaryKey == null || primaryKey.isEmpty()) {
                    primaryKey = UUID.randomUUID().toString();
                }
                values.setField(0, primaryKey);
                values.setField(1, obj.get("trip_id"));
                values.setField(2, obj.get("id"));
                values.setField(3, obj.get("route_id"));
                values.setField(4, obj.get("trip_direction"));
                try {
                    values.setField(5, df.parse(actualTripStartTime).getTime()*1000);
                } catch (ParseException e) {
                    System.out.println("Error"+e);
                }
                values.setField(6, obj.get("last_stop_arrival_time").toString());
                values.setField(7, obj.get("vehicle_label"));
                values.setField(8, obj.get("license_plate"));
                values.setField(9, obj.get("last_stop_id"));
                values.setField(10, obj.get("speed"));
                try {
                    values.setField(11, df.parse(observationDateTime).getTime()*1000);
                } catch (ParseException e) {
                    System.out.println("Error"+e);
                }
                values.setField(12, obj.get("trip_delay").toString());
                values.setField(13,location.get("type"));
                values.setField(14,coordinates.get(0));
                values.setField(15,coordinates.get(1));
            }catch(Exception e){
                System.out.println("Error"+e);
            }
            return values;
        }
    }

    static class CustomRMQSource extends RMQSource<Row> {

        public CustomRMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName, boolean usesCorrelationId, DeserializationSchema<Row> deserializationSchema) {
            super(rmqConnectionConfig, queueName, usesCorrelationId, deserializationSchema);
        }

        @Override
        protected void setupQueue() {
        }
    }



    static class CustomKuduFailureHandler implements KuduFailureHandler {

<<<<<<< HEAD:flink-connectors/FlinkStreamingKuduRemoteRMQ/src/main/java/SuratITMSLive.java
        @Override
        public void onFailure(List<RowError> failure) throws IOException {
            // Doing nothing here, sigh
        }
    }

=======
      @Override
      public void onFailure(List<RowError> failure) throws IOException {
        // Doing nothing here, sigh
      }
    }
    
>>>>>>> ade4b3b8d35f76953d1ca6ad8123d70762262002:flink-connectors/FlinkStreamingKuduRemoteRMQ/src/main/java/RMQKuduSink.java
}
