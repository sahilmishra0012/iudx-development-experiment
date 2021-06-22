import org.apache.kudu.client.KuduClient;
import java.util.List;

public class KuduTableManager {
    KuduClient client;
    KuduTableManager(KuduClient client){
        this.client = client;
    }
    List<String> showTables() throws Exception {
        List<String> tables = this.client.getTablesList().getTablesList();
        return tables;
    }
    void dropTable(String tableName) throws Exception {
        client.deleteTable(tableName);
    }
    void dropTable(List<String> tables) throws Exception {
        for(String tab : tables){
            System.out.println(tab);
            client.deleteTable(tab);
        }
    }
/*
    Tester Main Function

    public static void main(String[] args) throws Exception {
        final String KUDU_MASTERS = System.getProperty("kuduMasters", "127.0.0.1:7051,127.0.0.1:7151,127.0.0.1:7251");
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
        KuduTableManager obj = new KuduTableManager(client);
        List<String> list = obj.showTables();
        for(String tab : list){
            System.out.println(tab);
            client.deleteTable(tab);
        }
    }
*/
}
