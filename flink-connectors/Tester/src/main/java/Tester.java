import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.failure.KuduFailureHandler;
import org.apache.flink.connectors.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.connector.writer.RowOperationMapper;
import org.apache.flink.connectors.kudu.streaming.KuduSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.RowError;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class Tester {

    private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "kudu-master:7051");

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //        Add the date part as current date
        final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String currentDate = sdf2.format(timestamp);
//        System.out.println(sdf2.format(timestamp)+"06:20:00");         // 2021-03-24T16:48:05.591+08:00

        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet request = new HttpGet("https://smcitms.in/avls/index.php/Smcapi/getschedule/format/json?tokenid=20200908");
        CloseableHttpResponse response = httpClient.execute(request);

        // Add Headers
        StringBuilder sb = new StringBuilder();

        if (response.getEntity() != null) {
            String result = EntityUtils.toString(response.getEntity());
            JSONArray j = new JSONArray(result);
            for(Object a : j){
                JSONObject row = new JSONObject(a.toString());
                JSONObject k = row.getJSONObject("trip_details");
                sb.append(currentDate+" "+k.get("arrival_time")+","+row.get("trip_id")+","+k.get("stop_sequence")+","+k.get("stop_id")+","+currentDate+" "+k.get("departure_time")+"\n");
            }
            PrintWriter writer = new PrintWriter("/tmp/data.csv");
            writer.write(sb.toString());
        }


        String tableName = "scheduler12112";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();

        try {
            createExampleTable(client, tableName);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Table already exist or has creation problem.");
        }

        DataStreamSource<String> cv = env.readTextFile("/tmp/data.csv");
        DataStream<Row> rows =
                cv.flatMap(new Tokenizer());

        // kudu sink
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder.setMasters("kudu-master:7051").build();
        try{
            KuduSink<Row> sink = new KuduSink<>(
                    writerConfig,
                    KuduTableInfo.forTable(tableName),
                    new RowOperationMapper(
                            new String[]{"arrival_time","trip_id","stop_sequence","stop_id","departure_time"},
                            AbstractSingleOperationMapper.KuduOperation.INSERT), new CustomKuduFailureHandler());
            rows.addSink(sink);
        }
        catch(Exception e){
            System.out.println("Duplicate Row");
            System.out.println(e);
        }


        env.execute("Hi");
    }
    public static final class Tokenizer  implements FlatMapFunction<String, Row> {

        @Override
        public void flatMap(String value, Collector<Row> out) {
            String[] tokens = value.split(",");
            out.collect(Row.of(tokens[0],Integer.parseInt(tokens[1]),Integer.parseInt(tokens[2]),Integer.parseInt(tokens[3]),tokens[4]));
            System.out.println(Row.of(tokens[0],Integer.parseInt(tokens[1]),Integer.parseInt(tokens[2]),Integer.parseInt(tokens[3]),tokens[4]));
        }
    }
    static void createExampleTable(KuduClient client, String tableName) throws KuduException {

        List<ColumnSchema> columns = new ArrayList<>(5);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("arrival_time", Type.STRING).key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("trip_id", Type.INT32)
                .nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("stop_sequence", Type.INT32).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("stop_id", Type.INT32).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("departure_time", Type.STRING).nullable(true)
                .build());
        Schema schema = new Schema(columns);

        CreateTableOptions cto = new CreateTableOptions();
        cto.setNumReplicas(1);
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("arrival_time");
        int numBuckets = 8;
        cto.addHashPartitions(hashKeys, numBuckets);

        // Create the table.
        client.createTable(tableName, schema, cto);
        System.out.println("Created table " + tableName);
    }
    static class CustomKuduFailureHandler implements KuduFailureHandler {

        @Override
        public void onFailure(List<RowError> failure) throws IOException {
            // Doing nothing here, sigh
        }
    }
}