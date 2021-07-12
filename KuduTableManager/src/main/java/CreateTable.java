import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RangePartitionBound;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;

class CreateTable {

    KuduClient client;
    String tableName;
    JSONArray columns;
    int replicas;
    Boolean waitKey;
    int buckets;
    JSONArray hashPartitionKeys;
    JSONArray rangePartitionKeys;
    JSONArray rangePartitions;
    Boolean hasHashPartitions=true;
    Boolean hasRangePartitions=true;


    CreateTable(KuduClient client) throws IOException {
        this.client = client;
        String content = new String(Files.readAllBytes(Paths.get("createTableConfig.json")));
        JSONObject data = new JSONObject(content);
        try{
            this.tableName = (String) data.getJSONObject("spec").getJSONObject("tableSchema").get("tableName");
            this.columns = data.getJSONObject("spec").getJSONObject("tableSchema").getJSONObject("dimensionsSpec").getJSONArray("dimensions");
            this.replicas = (int) data.getJSONObject("spec").getJSONObject("ioConfig").get("replicas");
            this.waitKey = (Boolean) data.getJSONObject("spec").getJSONObject("ioConfig").get("waitKey");
        }
        catch(Exception e)
        {
            System.err.println("Configuration file does not contain required properties.");
            System.exit(0);
        }
        try{
            this.buckets = (int) data.getJSONObject("spec").getJSONObject("tuningConfig").getJSONObject("partitionSpec").getJSONObject("hashPartition").get("buckets");
            this.hashPartitionKeys =  data.getJSONObject("spec").getJSONObject("tuningConfig").getJSONObject("partitionSpec").getJSONObject("hashPartition").getJSONArray("keys");
        }
        catch(Exception e)
        {
            this.hasHashPartitions = false;
        }
        try{
            this.rangePartitionKeys =  data.getJSONObject("spec").getJSONObject("tuningConfig").getJSONObject("partitionSpec").getJSONObject("rangePartition").getJSONArray("columns");
            this.rangePartitions =  data.getJSONObject("spec").getJSONObject("tuningConfig").getJSONObject("partitionSpec").getJSONObject("rangePartition").getJSONArray("partitions");
        }
        catch(Exception e)
        {
            this.hasRangePartitions = false;
        }
    }

    public static void addPartition(int index, PartialRow bound, Object partition)
    {
        if(partition.getClass()==String.class)
        {
            bound.addString(index, String.valueOf(partition));
        }
        else if(partition.getClass()==Integer.class)
        {
            bound.addInt(index, (Integer) partition);
        }
        else if(partition.getClass()==Double.class)
        {
            bound.addDouble(index, (Double) partition);
        }
        else if(partition.getClass()==Long.class)
        {
            bound.addLong(index, (Long) partition);
        }
        else if(partition.getClass()==Float.class)
        {
            bound.addFloat(index, (Float) partition);
        }
        else if(partition.getClass()== BigDecimal.class)
        {
            bound.addDecimal(index, (BigDecimal) partition);
        }
        else if(partition.getClass()== Timestamp.class)
        {
            bound.addTimestamp(index, (Timestamp) partition);
        }
        else if(partition.getClass()== Boolean.class)
        {
            bound.addBoolean(index, (Boolean) partition);
        }
        else{
            bound.addString(index, String.valueOf(partition));
        }
    }

    Schema createSchema()
    {
        List<ColumnSchema> dimensions = new ArrayList<>(this.columns.length());
        for(Object column: this.columns){
            JSONObject temp = new JSONObject(column.toString());
            if(Boolean.parseBoolean(temp.get("is_key").toString()))
            {
                dimensions.add(new ColumnSchema.ColumnSchemaBuilder(temp.get("name").toString(), Type.getTypeForName(temp.get("type").toString()))
                        .key(true)
                        .build());
            }
            else
            {
                dimensions.add(new ColumnSchema.ColumnSchemaBuilder(temp.get("name").toString(), Type.getTypeForName(temp.get("type").toString()))
                        .nullable(true)
                        .build());
            }
        }
        return new Schema(dimensions);
    }
    void addHashPartitions(CreateTableOptions cto)
    {
        List<String> hashPartitionColumns = new ArrayList<>(this.hashPartitionKeys.length());
        for(Object hashPartitionColumn: this.hashPartitionKeys){
            hashPartitionColumns.add(hashPartitionColumn.toString());
        }
        cto.addHashPartitions(hashPartitionColumns, this.buckets);

    }
    void addRangePartition(Schema schema, CreateTableOptions cto)
    {
        List<String> rangePartitionColumns = new ArrayList<>(this.rangePartitionKeys.length());
        for(Object rangePartitionColumn: this.rangePartitionKeys){
            rangePartitionColumns.add(rangePartitionColumn.toString());
        }
        cto.setRangePartitionColumns(rangePartitionColumns);

        for(Object rangePartition: this.rangePartitions){
            PartialRow lower = schema.newPartialRow();
            PartialRow upper = schema.newPartialRow();
            JSONArray lowerRange = new JSONObject(rangePartition.toString()).getJSONArray("lower");
            JSONArray upperRange = new JSONObject(rangePartition.toString()).getJSONArray("upper");
            for(int i=0;i<lowerRange.length();i++){
                addPartition(i,lower,lowerRange.get(i));
                addPartition(i,upper,upperRange.get(i));
            }
            RangePartitionBound lowerBound = new JSONObject(rangePartition.toString()).get("lower_bound").toString().equals("exclusive") ? RangePartitionBound.EXCLUSIVE_BOUND:RangePartitionBound.INCLUSIVE_BOUND;
            RangePartitionBound upperBound = new JSONObject(rangePartition.toString()).get("upper_bound").toString().equals("exclusive") ? RangePartitionBound.EXCLUSIVE_BOUND:RangePartitionBound.INCLUSIVE_BOUND;
            cto.addRangePartition(lower, upper, lowerBound, upperBound);
        }
    }
    void setReplicas(CreateTableOptions cto){
        cto.setNumReplicas(this.replicas);
    }
    void setWait(CreateTableOptions cto){
        cto.setWait(this.waitKey);
    }

    void createTable() throws IOException {
        if(!this.client.tableExists(this.tableName))
        {
            Schema schema = createSchema();
            CreateTableOptions cto = new CreateTableOptions();
            setReplicas(cto);
            setWait(cto);
            if(this.hasHashPartitions || this.hasRangePartitions)
            {
                if(this.hasHashPartitions)
                {
                    addHashPartitions(cto);
                }
                if(this.hasRangePartitions)
                {
                    addRangePartition(schema, cto);
                }
                this.client.createTable(this.tableName, schema, cto);
                System.out.println("Created table " + this.tableName);
            }
            else
            {
                System.err.println("Table partitioning must be specified using setRangePartitionColumns or addHashPartitions");
            }
        }
        else{
            System.err.println("Table "+this.tableName+" already exists");
        }
    }
}
