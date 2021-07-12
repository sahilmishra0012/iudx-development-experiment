import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.writer.AbstractSingleOperationMapper;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.connector.writer.RowOperationMapper;
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


public class RMQKuduSink {

    private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "127.0.0.1:7051");

    public static void main(String[] args) throws Exception {
        KuduCatalog catalog = new KuduCatalog(KUDU_MASTERS);
        tableEnv.registerCatalog("kudu", catalog);
        tableEnv.useCatalog("kudu");
        tableEnv.listTables();
    }

}
