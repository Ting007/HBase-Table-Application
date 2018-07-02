package cs685.hm3;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;

import com.csvreader.*;
import org.apache.commons.csv.*;

public class InsertData {

	public static void main(String[] args) throws IOException {
		
		Configuration conf = HBaseConfiguration.create();
		
		conf.setInt("timeout", 120000);
	    conf.set("hbase.master", "*" + "127.0.2.3:8020");
	    conf.set("hbase.zookeeper.quorum", "192.168.56.101");
	    conf.set("hbase.zookeeper.property.clientPort", "2181");
		Connection conn = ConnectionFactory.createConnection(conf);
		
		Admin admin = conn.getAdmin();
		// TODO Auto-generated method stub
		//Instantiating HTable class
        //HTable hTable = new HTable(conf, "pts");
        //start reading files from p2.csv
		TableName tableName = TableName.valueOf("pts");
		Table hTable = conn.getTable(tableName);
        CsvReader patient1 = new CsvReader("/home/cloudera/sample_data/p2.csv");
        patient1.readHeaders();
        
        int r_id = 1;

        while (patient1.readRecord())
        {   
        	String row = "row" + Integer.toString(r_id);
        	Put p = new Put(Bytes.toBytes(row));
        	//initialize the input variable for table
            String pID = patient1.get("pid");
            if (pID != "NULL"){
                p.add(Bytes.toBytes("other"), Bytes.toBytes("pid"), Bytes.toBytes(pID));
            }
            
            
            String gender = patient1.get("gender");
            if(gender == "1"){
                p.add(Bytes.toBytes("demographics"), Bytes.toBytes("gender"), Bytes.toBytes("Male"));
            }
            if(gender == "2"){
            	p.add(Bytes.toBytes("demographics"), Bytes.toBytes("gender"), Bytes.toBytes("Female"));
            }
            
            String race = patient1.get("race");
            if (race != "NULL"){
                p.add(Bytes.toBytes("demographics"), Bytes.toBytes("race"), Bytes.toBytes(race));
            }
            		
            String height = patient1.get("height");
            if (height != "NULL"){
                p.add(Bytes.toBytes("anthropometry"), Bytes.toBytes("height"), Bytes.toBytes(height));
            }
            String weight = patient1.get("weight");
            if (weight != "NULL"){
                p.add(Bytes.toBytes("anthropometry"), Bytes.toBytes("weight"), Bytes.toBytes(weight));
            }
            
            String asthma = patient1.get("asthma");
            if (asthma == "0"){
                p.add(Bytes.toBytes("medical_history"), Bytes.toBytes("asthma"), Bytes.toBytes("No"));
            }
            if (asthma == "1"){
            	p.add(Bytes.toBytes("medical_history"), Bytes.toBytes("asthma"), Bytes.toBytes("Yes"));
            }
            if (asthma == "8"){
            	p.add(Bytes.toBytes("medical_history"), Bytes.toBytes("asthma"), Bytes.toBytes("Unknown"));
            }
            
            String hypertension = patient1.get("hypertension");
            if (hypertension != "NULL"){
            	p.add(Bytes.toBytes("medical_history"), Bytes.toBytes("hypertension"), Bytes.toBytes("hypertension"));
            }
            
            String year = patient1.get("year");
            if (year != "NULL"){
                p.add(Bytes.toBytes("other"), Bytes.toBytes("year"), Bytes.toBytes(year));
            }
            hTable.put(p);
            r_id += 1;
        }

        patient1.close();
        System.out.println("p2.csv loaded.");
        
        CsvReader patient2 = new CsvReader("/home/cloudera/sample_data/p2.csv");
        patient2.readHeaders();
        
        while (patient2.readRecord())
        {   
        	String row = "row" + Integer.toString(r_id);
        	Put p = new Put(Bytes.toBytes(row));
        	//initialize the input variable for table
            String pID = patient2.get("pid");
            if (pID != "NULL"){
                p.add(Bytes.toBytes("other"), Bytes.toBytes("pid"), Bytes.toBytes(pID));
            }
            
            
            String gender = patient2.get("gender");
            if(gender == "1"){
                p.add(Bytes.toBytes("demographics"), Bytes.toBytes("gender"), Bytes.toBytes("Male"));
            }
            if(gender == "0"){
            	p.add(Bytes.toBytes("demographics"), Bytes.toBytes("gender"), Bytes.toBytes("Female"));
            }
            
            String race = patient2.get("race");
            if (race != "NULL"){
                p.add(Bytes.toBytes("demographics"), Bytes.toBytes("race"), Bytes.toBytes(race));
            }
            		
            String height = patient2.get("height");
            if (height != "NULL"){
                p.add(Bytes.toBytes("anthropometry"), Bytes.toBytes("height"), Bytes.toBytes(height));
            }
            String weight = patient2.get("weight");
            if (weight != "NULL"){
                p.add(Bytes.toBytes("anthropometry"), Bytes.toBytes("weight"), Bytes.toBytes(weight));
            }
            
            String asthma = patient2.get("asthma");
            if (asthma == "2"){
                p.add(Bytes.toBytes("medical_history"), Bytes.toBytes("asthma"), Bytes.toBytes("No"));
            }
            if (asthma == "1"){
            	p.add(Bytes.toBytes("medical_history"), Bytes.toBytes("asthma"), Bytes.toBytes("Yes"));
            }
            if (asthma == "8"){
            	p.add(Bytes.toBytes("medical_history"), Bytes.toBytes("asthma"), Bytes.toBytes("Unknown"));
            }
            
            String hypertension = patient2.get("hypertension");
            if (hypertension != "NULL"){
            	p.add(Bytes.toBytes("medical_history"), Bytes.toBytes("hypertension"), Bytes.toBytes("hypertension"));
            }
            
            String year = patient2.get("year");
            if (year != "NULL"){
                p.add(Bytes.toBytes("other"), Bytes.toBytes("year"), Bytes.toBytes(year));
            }
            hTable.put(p);
            r_id += 1;
        }

        patient2.close();
        System.out.println("q3.csv loaded");

	}

}

