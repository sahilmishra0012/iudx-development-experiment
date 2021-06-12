import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.json.JSONObject;

import javax.xml.bind.DatatypeConverter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

class FlinkRMQHiveSink {
    static class CustomRMQSource extends RMQSource<Row> {
        public CustomRMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName, boolean usesCorrelationId, DeserializationSchema<Row> deserializationSchema) {
            super(rmqConnectionConfig, queueName, usesCorrelationId, deserializationSchema);
        }
        @Override
        protected void setupQueue() {
        }
    }

    static class CustomDeserializationSchema extends AbstractDeserializationSchema<Row> {
        @Override
        public Row deserialize(byte[] bytes) {

            String base64Encoded = DatatypeConverter.printBase64Binary(bytes);
            System.out.println("Encoded Json: " + base64Encoded);

            byte[] base64Decoded = DatatypeConverter.parseBase64Binary(base64Encoded);
            String streamData = new String(base64Decoded);
            System.out.println("Decoded Json: " + streamData);
            JSONObject obj = new JSONObject(streamData);
            System.out.println("JsonObject: " + obj.get("name"));
            Row values = new Row(2);

            values.setField(0, obj.get("date"));
            values.setField(1, obj.get("name"));

            return values;
        }
    }

    public static void main(String []args) throws Exception {

        EnvironmentSettings settings= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000 * 5);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //Hive Configurations
        String name = "hive";
        String defaultDatabase = "default";
        String hiveConfDir = "/home/samkiller007/apache-hive-2.3.8-bin/conf"; // Path to Hive Conf directory

        // generating properties for the Input Stream - RMQ
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

        // Fetching input data flowing from RQM through the source stream.
        final DataStream<Row> stream = env
                .addSource(new CustomRMQSource(
                        connectionConfig,
                        "adaptor-test",
                        false,
                        new CustomDeserializationSchema()
                )).setParallelism(1).returns(new RowTypeInfo(Types.STRING, Types.STRING));;

        // Hive Catalogue Creation
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);

        // Registering catalogue
        tableEnv.registerCatalog("hive", hive);

        // Select catalogue for usage
        tableEnv.useCatalog("hive");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        try {

            // Create Database to store external table and temporary table
            hive.createDatabase("mydb", new CatalogDatabaseImpl(new HashMap<>(), ""), true);
            tableEnv.useDatabase("mydb");
            // Temporary Table to store streaming data
            tableEnv.createTemporaryView("my_table", stream);

            // External table to store data permanently in Hive Metastore
            tableEnv.executeSql("create external table if not exists sink_table (a string,b string)");

            // Writing data to external table
            tableEnv.sqlQuery("select * from my_table").executeInsert("sink_table");

            // Start the execution of the worker
            // env.execute("flinkjob");
        }
        catch (Exception e){
            System.out.println(e);
        }
//        finally {
//            tableEnv.executeSql("drop database mydb cascade");
//        }
    }

}