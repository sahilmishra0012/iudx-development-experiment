import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.json.JSONObject;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class FlinkRMQParquetSink {
    public static void main(String[] args) throws Exception {

        // creating environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // generating properties for the Input Stream
        env.setParallelism(1);

        // checkpoint
        env.enableCheckpointing(10000);
        Properties properties = new Properties();
        try {
            InputStream inputStream = new FileInputStream("dataConfig.properties");
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }


        // Creating connection configuration for RMQ Source
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(properties.getProperty("hostName"))
                .setUserName(properties.getProperty("username"))
                .setPassword(properties.getProperty("password"))
                .setPort(Integer.parseInt(properties.getProperty("port")))
                .setVirtualHost(properties.getProperty("virtualHost"))
                .build();
        Schema schema = new Schema.Parser().parse(new File("avro.avsc"));
        // Fetching input data flowing from RQM through the source stream.
        final DataStream<GenericRecord> stream = env
                .addSource(new CustomRMQSource(
                        connectionConfig,
                        "adaptor-test",
                        false,
                        new CustomDeserializationSchema()
                )).returns(new GenericRecordAvroTypeInfo(schema));

        final StreamingFileSink<GenericRecord> sink = StreamingFileSink
                .forBulkFormat(new Path("data"), ParquetAvroWriters.forGenericRecord(schema)).withBucketAssigner(new EventTimeBucketAssigner()).withBucketCheckInterval(100000).withRollingPolicy(
                        OnCheckpointRollingPolicy.build())
                .build();

        stream.addSink(sink);

        env.execute("flink_test_job");

    }

    static class CustomDeserializationSchema extends AbstractDeserializationSchema<GenericRecord> {

        @Override
        public GenericRecord deserialize(byte[] bytes) throws IOException {

            String base64Encoded = DatatypeConverter.printBase64Binary(bytes);
            System.out.println("Encoded Json: " + base64Encoded);

            byte[] base64Decoded = DatatypeConverter.parseBase64Binary(base64Encoded);
            String streamData = new String(base64Decoded);
            System.out.println("Decoded Json: " + streamData);
            JSONObject obj = new JSONObject(streamData);
            System.out.println("JsonObject: " + obj.get("name"));

            Schema schema = new Schema.Parser().parse(new File("avro.avsc"));
            GenericRecord values = new GenericData.Record(schema);
            values.put("message", obj.get("name"));
            values.put("date", obj.get("date"));
            System.out.println(values);
            System.out.println("**********************************************\n");

            return values;
        }
    }

    static class CustomRMQSource extends RMQSource<GenericRecord> {

        public CustomRMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName, boolean usesCorrelationId, DeserializationSchema<GenericRecord> deserializationSchema) {
            super(rmqConnectionConfig, queueName, usesCorrelationId, deserializationSchema);
        }

        @Override
        protected void setupQueue() {
        }
    }

    public static class EventTimeBucketAssigner implements BucketAssigner<GenericRecord, String> {
        private ObjectMapper mapper = new ObjectMapper();

        @Override
        public String getBucketId(GenericRecord element, Context context) {
            String partitionValue;
            try {
                partitionValue = (String) element.get("date");
            } catch (Exception e) {
                partitionValue = "00000000";
            }
            return "partitions/" + partitionValue;
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }

        private Date getPartitionValue(GenericRecord element) throws Exception {
            return new SimpleDateFormat().parse((String) element.get("date"));
        }
    }
}
