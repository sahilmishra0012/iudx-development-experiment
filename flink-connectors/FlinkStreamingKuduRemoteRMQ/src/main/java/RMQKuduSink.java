import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.connectors.kudu.streaming.KuduSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.types.Row;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.xml.bind.DatatypeConverter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;


public class RMQKuduSink {

    private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "127.0.0.1:7051");

    public static void main(String[] args) throws Exception {

        // creating environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
                        true,
                        new CustomDeserializationSchema()
                ))
                .setParallelism(1);



        String tableName = "surat_data89";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();

        try {
            createExampleTable(client, tableName);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Table already exist or has creation problem.");
        }


        // kudu sink
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters("127.0.0.1:7051").build();

        KuduSink<Row> sink = new KuduSink<>(
                writerConfig,
                KuduTableInfo.forTable(tableName),
                new RowOperationMapper(
                        new String[]{"trip_id","id","route_id","trip_direction","last_stop_arrival_time","vehicle_label","license_plate","last_stop_id","speed","trip_delay","location_type","coordinate0","coordinate1"},
                        AbstractSingleOperationMapper.KuduOperation.UPSERT));

        stream.addSink(sink);

        env.execute("flink_test_job");

    }

    static void createExampleTable(KuduClient client, String tableName) throws KuduException {

        List<ColumnSchema> columns = new ArrayList<>(13);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_id", Type.STRING)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("route_id", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_direction", Type.STRING).nullable(true)
                .build());
//        columns.add(new ColumnSchema.ColumnSchemaBuilder("actual_trip_start_time", Type.UNIXTIME_MICROS).nullable(true)
//                .build());
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
//        columns.add(new ColumnSchema.ColumnSchemaBuilder("observationDateTime", Type.UNIXTIME_MICROS).nullable(true)
//                .build());
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
        hashKeys.add("trip_id");
        int numBuckets = 8;
        cto.addHashPartitions(hashKeys, numBuckets);

        // Create the table.
        client.createTable(tableName, schema, cto);
        System.out.println("Created table " + tableName);
    }


    static class CustomDeserializationSchema extends AbstractDeserializationSchema<Row> {

        @Override
        public Row deserialize(byte[] bytes) {

            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            df.setTimeZone(TimeZone.getTimeZone("UTC"));

            String base64Encoded = DatatypeConverter.printBase64Binary(bytes);

            byte[] base64Decoded = DatatypeConverter.parseBase64Binary(base64Encoded);
            String streamData = new String(base64Decoded);

            JSONObject obj = new JSONObject(streamData);
            JSONObject location = obj.getJSONObject("location");
            JSONArray coordinates = location.getJSONArray("coordinates");
//            String actualTripStartTime =  obj.get("actual_trip_start_time").toString();
//            String observationDateTime =  obj.get("observationDateTime").toString();

            Row values = new Row(13);
            values.setField(0, obj.get("trip_id"));
            values.setField(1, obj.get("id"));
            values.setField(2, obj.get("route_id"));
            values.setField(3, obj.get("trip_direction"));
//            try {
//                values.setField(4, df.parse(actualTripStartTime).getTime()*1000);
//            } catch (ParseException e) {
//                System.out.println("Error"+e);
//            }
            values.setField(4, obj.get("last_stop_arrival_time").toString());
            values.setField(5, obj.get("vehicle_label"));
            values.setField(6, obj.get("license_plate"));
            values.setField(7, obj.get("last_stop_id"));
            values.setField(8, obj.get("speed"));
//            try {
//                values.setField(10, df.parse(observationDateTime).getTime()*1000);
//            } catch (ParseException e) {
//                System.out.println("Error"+e);
//            }
            values.setField(9, obj.get("trip_delay").toString());
            values.setField(10,location.get("type"));
            values.setField(11,coordinates.get(0));
            values.setField(12,coordinates.get(1));

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
}