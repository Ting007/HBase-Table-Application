package cs685.hm3;

import java.io.IOException;

/**
 * Hello world!
 *
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class CreateTable {

	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		//conf.setInt("timeout", 120000);
	    //conf.set("hbase.master", "*" + "127.0.2.3:8020");
	    //conf.set("hbase.zookeeper.quorum", "192.168.56.101");
	    //conf.set("hbase.zookeeper.property.clientPort", "2181");
		Connection conn = ConnectionFactory.createConnection(conf);
	    
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("pts"));
        tableDescriptor.addFamily(new HColumnDescriptor("demographics"));
        tableDescriptor.addFamily(new HColumnDescriptor("anthropometry"));
        tableDescriptor.addFamily(new HColumnDescriptor("medical_history"));
        tableDescriptor.addFamily(new HColumnDescriptor("other"));
        admin.createTable(tableDescriptor);
        //boolean tableAvailable = admin.isTableAvailable("pts");
        System.out.println(" Table created ");
        
    }
	
}
