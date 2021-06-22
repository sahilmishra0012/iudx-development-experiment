
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
import org.apache.kudu.client.*;
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
import org.apache.kudu.shaded.com.google.common.collect.Lists;


import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.apache.kudu.client.SessionConfiguration.FlushMode;
import org.apache.kudu.shaded.com.google.common.collect.Table;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.xml.bind.DatatypeConverter;



public class RMQKuduSink {


    private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251");
    public static void main(String[] args) throws Exception {

        // creating environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // Creating connection configuration for RMQ Source
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("rmq_rmq_1")
                .setUserName("guest")
                .setPassword("guest")
                .setPort(5672)
                .setVirtualHost("/")
                .build();

        // Fetching input data flowing from RQM through the source stream.
        final DataStream<Row> stream = env
                .addSource(new CustomRMQSource(
                        connectionConfig,
                        "adaptor-test",
                        true,
                        new CustomDeserializationSchema()
                        ))
                .setParallelism(1);


        String tableName = "nuemann2";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();

        try {
            createExampleTable(client, tableName);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Table already exist or has creation problem.");
        }


        // kudu sink
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters("kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251").build();
        KuduSink<Row> sink = new KuduSink<>(
                writerConfig,
                KuduTableInfo.forTable(tableName),
                new RowOperationMapper(
                        new String[]{"key", "date", "signal"},
                        AbstractSingleOperationMapper.KuduOperation.UPSERT));

        stream.addSink(sink);

        env.execute("flink_test_job");

    }

    static void createExampleTable(KuduClient client, String tableName) throws KuduException {

        List<ColumnSchema> columns = new ArrayList<>(3);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("date",Type.STRING ).key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("signal", Type.DOUBLE).nullable(true)
                .build());
        Schema schema = new Schema(columns);
        CreateTableOptions cto = new CreateTableOptions();
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("key");
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

            System.out.println("KEY: " + obj.get("key"));
            System.out.println("DATE: " + obj.get("date"));
            System.out.println("SIGNAL: " + obj.get("signal"));
            System.out.println("**********************************************\n");

            Row values = new Row(3);
            values.setField(0, obj.get("key"));
            values.setField(1, obj.get("date"));
            values.setField(2, obj.get("signal"));
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
