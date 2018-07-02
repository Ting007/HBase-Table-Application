package cs685.hm3;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;

public class ReadData {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
        Configuration conf = HBaseConfiguration.create();
		
//		conf.setInt("timeout", 120000);
//	    conf.set("hbase.master", "*" + "127.0.2.3:8020");
//	    conf.set("hbase.zookeeper.quorum", "192.168.56.101");
//	    conf.set("hbase.zookeeper.property.clientPort", "2181");
		Connection conn = ConnectionFactory.createConnection(conf);
		
		Admin admin = conn.getAdmin();
		// TODO Auto-generated method stub
		//Instantiating HTable class
        //HTable hTable = new HTable(conf, "pts");
        //start reading files from p2.csv
		TableName tableName = TableName.valueOf("pts");
		Table hTable = conn.getTable(tableName);
		
		Scan s = new Scan();
		s.addColumn(Bytes.toBytes("demographics"), Bytes.toBytes("gender"));
		ResultScanner scanner = hTable.getScanner(s);
		int female = 0;
		int male = 0;
		try{
			for (Result rr = scanner.next(); rr != null; rr = scanner.next()){
				byte[] bytes = rr.getValue(Bytes.toBytes("demographics"), Bytes.toBytes("gender"));
				String value = Bytes.toString(bytes);
				System.out.println(value);
				if (value == "Female"){
					female += 1;
				}
				if (value == "Male"){
					male += 1;
				}
			}
		}finally{
			scanner.close();
		}
		System.out.println("The total numver of female patient is " + Integer.toString(female));
		System.out.println("The total numver of male patient is " + Integer.toString(male));
	}

}
