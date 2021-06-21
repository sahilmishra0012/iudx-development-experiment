
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kudu.client.KuduClient;
import org.apache.flink.connectors.kudu.streaming.KuduSink;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;

import org.apache.flink.connectors.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.types.Row;

//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.TableEnvironment;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.shaded.com.google.common.collect.Lists;


import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.SessionConfiguration.FlushMode;
import org.apache.kudu.shaded.com.google.common.collect.Table;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.xml.bind.DatatypeConverter;



public class RMQKuduSink {

    private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "127.0.0.1:7051,127.0.0.1:7151,127.0.0.1:7251");

    public static void main(String[] args) throws Exception {

        // creating environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        // generating properties for the Input Stream
        Properties properties = new Properties();
        try {
            InputStream inputStream = new FileInputStream("dataConfig.properties");
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(properties.getProperty("hostName"));
        System.out.println(properties.getProperty("username"));
        System.out.println(properties.getProperty("password"));
        System.out.println(properties.getProperty("port"));
        System.out.println(properties.getProperty("virtualHost"));

        // Creating connection configuration for RMQ Source
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(properties.getProperty("hostName"))
                .setUserName(properties.getProperty("username"))
                .setPassword(properties.getProperty("password"))
                .setPort(Integer.parseInt(properties.getProperty("port")))
                .setVirtualHost(properties.getProperty("virtualHost"))
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



        String tableName = "surat55858673";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();

        try {
            createExampleTable(client, tableName);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Table already exist or has creation problem.");
        }


        // kudu sink
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters("127.0.0.1:7051,127.0.0.1:7151,127.0.0.1:7251").build();

        KuduSink<Row> sink = new KuduSink<>(
                writerConfig,
                KuduTableInfo.forTable(tableName),
                new RowOperationMapper(
                        new String[]{"trip_id","id","route_id","trip_direction","actual_trip_start_time","last_stop_arrival_time","vehicle_label","license_plate","last_stop_id","speed","observationDateTime","trip_delay"},
                        AbstractSingleOperationMapper.KuduOperation.UPSERT));

        stream.addSink(sink);

        env.execute("flink_test_job");

    }

    static void createExampleTable(KuduClient client, String tableName) throws KuduException {

        List<ColumnSchema> columns = new ArrayList<>(12);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_id", Type.STRING)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("route_id", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_direction", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("actual_trip_start_time", Type.STRING).nullable(true)
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
        columns.add(new ColumnSchema.ColumnSchemaBuilder("observationDateTime", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_delay", Type.INT32).nullable(true)
                .build());
        Schema schema = new Schema(columns);

        CreateTableOptions cto = new CreateTableOptions();
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
        public Row deserialize(byte[] bytes) throws IOException {

            String base64Encoded = DatatypeConverter.printBase64Binary(bytes);
            System.out.println("Encoded Json: " + base64Encoded);

            byte[] base64Decoded = DatatypeConverter.parseBase64Binary(base64Encoded);
            String streamData = new String(base64Decoded);
            System.out.println("Decoded Json: " + streamData + "\n");

            JSONObject obj = new JSONObject(streamData);

            System.out.println("**********************************************\n");

            Row values = new Row(12);
            values.setField(0, obj.get("trip_id"));
            values.setField(1, obj.get("id"));
            values.setField(2, obj.get("route_id"));
            values.setField(3, obj.get("trip_direction"));
            values.setField(4, obj.get("actual_trip_start_time"));
            values.setField(5, obj.get("last_stop_arrival_time").toString());
            values.setField(6, obj.get("vehicle_label"));
            values.setField(7, obj.get("license_plate"));
            values.setField(8, obj.get("last_stop_id"));
            values.setField(9, obj.get("speed"));
            values.setField(10, obj.get("observationDateTime"));
            values.setField(11, obj.get("trip_delay"));
            System.out.println(values.getField(5).getClass().getName());
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