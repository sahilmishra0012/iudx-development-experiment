import org.apache.flink.connectors.kudu.table.KuduCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.http.HttpHeaders;
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
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.TimeZone;


public class RMQKuduSink {

    private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "kudu_kudu-master-1_1:7051");


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

    public static void main(String[] args) throws Exception {
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
        sb.append("arrival_time,");
        sb.append("trip_id,");
        sb.append("stop_sequence,");
        sb.append("stop_id,");
        sb.append("departure_time\n");


        if (response.getEntity() != null) {
            String result = EntityUtils.toString(response.getEntity());
            JSONArray j = new JSONArray(result);
            for(Object a : j){
                JSONObject row = new JSONObject(a.toString());
                JSONObject k = row.getJSONObject("trip_details");
                sb.append(currentDate+" "+k.get("arrival_time")+","+row.get("trip_id")+","+k.get("stop_sequence")+","+k.get("stop_id")+","+currentDate+" "+k.get("departure_time")+"\n");
            }
            PrintWriter writer = new PrintWriter(new File("data.csv"));
            writer.write(sb.toString());
        }

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));






        String tableName = "scheduler";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();

        try {
            createExampleTable(client, tableName);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Table already exist or has creation problem.");
        }

        EnvironmentSettings settings= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        KuduCatalog catalog = new KuduCatalog(KUDU_MASTERS);
        tableEnv.registerCatalog("kudu", catalog);
        tableEnv.useCatalog("kudu");
        Scanner sc = new Scanner(new File("data.csv"));
        sc.useDelimiter(",");   //sets the delimiter pattern
        sc.nextLine();
        while (sc.hasNextLine())  //returns a boolean value
        {
            String [] k = sc.nextLine().split(",");  //find and returns the next complete token from this scanner
            String query = "insert into scheduler values('"+k[0]+"',"+k[1]+","+k[2]+","+k[3]+",'"+k[4]+"')";
            tableEnv.executeSql(query);
        }
    }

}