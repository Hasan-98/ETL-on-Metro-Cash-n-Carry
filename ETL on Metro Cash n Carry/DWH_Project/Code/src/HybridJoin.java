
import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;




















public class HybridJoin {
	
	
	/** The name of the MySQL account to use (or empty for anonymous) */
	private final String userName = "root";

	/** The password for the MySQL account (or empty for anonymous) */
	private final String password = "hamza123";

	/** The name of the computer running MySQL */
	private final String serverName = "localhost";

	/** The port of the MySQL server (default is 3306) */
	private final int portNumber = 3306;

	/** The name of the database we are testing with (this default is installed with MySQL) */
	private final String dbName = "metro_db";
	private final String dbName2 = "metro_dw";

	 static final String DB_URL = "jdbc:mysql://localhost/metro_db";
	 static final String DB_URL2 = "jdbc:mysql://localhost/metro_dw";
	 
	 ///////      RUN TIME DATA FOR MULTI-HASH FUNCTION          /////////////
	 
	 
	 public Multimap<String, store> myMap	;
	 public ArrayList<String> queueConnection;
	 public ArrayList<masterData> metaDataStorage;
	 public int line;
	 public ArrayList<saleFact> fnode;
	 public Connection conn3;
	 public ArrayList<String> dim_customer;
	 public ArrayList<String> dim_supplier;
	 public ArrayList<String> dim_product;
	 public ArrayList<String> dim_store;
	 public int prod;
	
	
	
	 public Connection getConnection() throws SQLException, ClassNotFoundException {
			
			Connection conn = null;
		//	MakeDB();
			Properties connectionProps = new Properties();
			connectionProps.put("user", this.userName);
			connectionProps.put("password", this.password);
			System.out.println("trying to get connection!! ");
		//	 conn = DriverManager.getConnection(DB_URL, userName, password);
			conn = DriverManager.getConnection("jdbc:mysql://"
			+ this.serverName + ":" + this.portNumber + "/" 
			+ this.dbName,connectionProps);		
			System.out.println(" Connection achieved!! ");
			return conn;
		}
	 
	 
	 
		
public Connection getConnection2() throws SQLException, ClassNotFoundException {
	 		
	 		Connection conn = null;
	 	//	MakeDB();
	 		Properties connectionProps = new Properties();
	 		connectionProps.put("user", this.userName);
	 		connectionProps.put("password", this.password);
	 		System.out.println("trying to get connection!! ");
	 	//	 conn = DriverManager.getConnection(DB_URL, userName, password);
	 		conn = DriverManager.getConnection("jdbc:mysql://"
	 		+ this.serverName + ":" + this.portNumber + "/" 
	 		+ this.dbName2,connectionProps);		
	 		System.out.println(" Connection achieved!! ");
	 		return conn;
}
	 	
	 
	 
public boolean executeUpdate(Connection conn, String command) throws SQLException {
		    Statement stmt = null;
		    try {
		        stmt = conn.createStatement();
		        stmt.executeUpdate(command); // This will throw a SQLException if it fails
		        return true;
		    } finally {

		    	// This will run whether we throw an exception or not
		        if (stmt != null) { stmt.close(); }
		    }
}
		
		
		
		
		
public void run() throws ClassNotFoundException, SQLException, ParseException {

			// Connect to MySQL
		//	MakeDB();
			Connection conn = null;
			fnode=new  ArrayList<saleFact>();
			dim_customer=new  ArrayList<String>();
			dim_supplier=new  ArrayList<String>();
			dim_product=new  ArrayList<String>();
			dim_store=new ArrayList<String>();
		//	dim_store=new  ArrayList<Sring>();
			
			

			try {
				conn = this.getConnection();
				System.out.println("connection name is :: "+conn.getClass().getName());
				System.out.println("Connected to database");
			} catch (SQLException e) {
				System.out.println("ERROR: Could not connect to the database");
				e.printStackTrace();
				return;
			}
			
			
			System.out.println("---------------------------------");
			
			//Connection conn2 = null;

			try {
				conn3 = this.getConnection2();
				//System.out.println("connection name is :: "+conn.getClass().getName());
			//	System.out.println("Connected to database");
			} catch (SQLException e) {
			//	System.out.println("ERROR: Could not connect to the database");
				e.printStackTrace();
				return;
			}
			
			
			
			
			
			
			dataStorage(conn);
			metaDataStorageHandler(conn);
			hybridAlgo();

}
		
		
		
public void dataStorage(Connection conn) throws ParseException
{
			myMap = HashMultimap.create();
			queueConnection=new ArrayList<String>();
			
			Statement stmt=null;
			try {
				stmt=conn.createStatement();
				
				String sql="SELECT * from transactions";
				ResultSet rs = stmt.executeQuery(sql);
				
				
				
				while(rs.next()){
					
					store temp=new store();
					// Date date1=new SimpleDateFormat("dd/MM/yyyy").parse(sDate1);  
					temp.TRANSACTION_ID= rs.getString("TRANSACTION_ID");
					temp.CUSTOMER_ID= rs.getString("CUSTOMER_ID");
					temp.CUSTOMER_NAME=rs.getString("CUSTOMER_NAME");
					temp.QUANTITY=rs.getInt("QUANTITY");
					
					
					temp.T_DATE=rs.getDate("T_Date");
					temp.STORE_NAME=rs.getString("STORE_NAME");
					temp.PRODUCT_ID=rs.getString("PRODUCT_ID");
					temp.STORE_ID=rs.getString("STORE_ID");
					
					///////////  queue insertion
					queueConnection.add(temp.PRODUCT_ID);
					///////////
					
					temp.pointer=queueConnection.size()-1;
					
					myMap.put(temp.PRODUCT_ID, temp);
					
					
			       
					 System.out.println(" customer name: " + temp.TRANSACTION_ID);
					//int a=rs.getInt("count(CUSTOMER_ID)");
					//System.out.print(" Employee name: " + a);
			      /*   String productname = rs.getString("Name");
			         String employeename = rs.getString("Name");

			         System.out.println(" Product name: " + productname);
			         System.out.print(" Employee name: " + employeename);*/
			      }
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			
}
	
		
		
 public void metaDataStorageHandler(Connection conn)
{
				
				
				metaDataStorage=new ArrayList<masterData>();
				
				Statement stmt=null;
				try {
					stmt=conn.createStatement();
					
					String sql="SELECT * from masterdata";
					ResultSet rs = stmt.executeQuery(sql);
					
					
					
					while(rs.next()){
						
						masterData temp=new masterData();
						
						temp.PRODUCT_ID= rs.getString("PRODUCT_ID");
						temp.PRODUCT_NAME= rs.getString("PRODUCT_NAME");
						temp.SUPPLIER_ID=rs.getString("SUPPLIER_ID");
						temp.PRICE=rs.getInt("PRICE");
						temp.SUPPLIER_NAME=rs.getString("SUPPLIER_NAME");
						
						
						///////////  queue insertion
						metaDataStorage.add(temp);
						///////////
						//System.out.println(" customer name: " + temp.PRODUCT_ID);
				      }
					
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				
				}
}
			
	
public masterData foundMatch(String m)
{
		    	for(int a=0;a<metaDataStorage.size();a++)
		    	{
		    		//System.out.println("comparing parameter="+m+"  with master data="+metaDataStorage.get(a).PRODUCT_ID);
		    		if(metaDataStorage.get(a).PRODUCT_ID.equals(m))
		    		{
		    		//	System.out.println("match has been found successfully");
		    			return (metaDataStorage.get(a));
		    		}
		    	}
		    	
		    	masterData d=new masterData();
		    	return d;
}
		 
		
		    
		
 public void dimCustomer(String cname,String cid)
		    {
		    	
				Statement stmt=null;
				try {
					stmt=conn3.createStatement();
					String sql = "INSERT  into metro_dw.customer_dim_table values("
							+ "\""+cid+"\", "
							+ "\""+cname+"\");" ;
					
					 stmt.executeUpdate(sql);
					 
				}
				catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    		
 }
		    
		    
		    
 public void dimProduct(String pname,String pid,int p)
 {
 	         Statement stmt=null;
		    try {
			stmt=conn3.createStatement();
			String sql = "INSERT  into metro_dw.product_dim_table values("
					+ "\""+pid+"\", "
					+ "\""+pname+"\", "
					+ "\""+p+"\");" ;
			
			 stmt.executeUpdate(sql);
			 
		    }
		    catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 	
 	
  }	  
 
 
 public void dimStore(String storename,String storeid)
 {
 	Statement stmt=null;
		try {
			stmt=conn3.createStatement();
			String sql = "INSERT  into metro_dw.store_dim_table values("
					+ "\""+storeid+"\", "
					+ "\""+storename+"\");" ;
			
			 stmt.executeUpdate(sql);
			 
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 	
 }
 
 public void dimSupplier(String sname,String sid)
 {
		
		Statement stmt=null;
		try {
			stmt=conn3.createStatement();
			
		//	String price=Integer.toString(f.PRICE);
		//	String t_sale=Integer.toString(f.TOTAL_SALE);
		//	String quantity=Integer.toString(f.QUANTITY);
			
			//String sql="INSERT INTO fact_table(transaction_id, customer_id, product_id, store_id, supplier_id, time_id,quantity,price,sale)\r\n"
				//	+ "VALUES("+f.TRANSACTION_ID+","+f.CUSTOMER_ID+","+ f.PRODUCT_ID+","+f.STORE_ID+","+f.SUPPLIER_ID+","+f.T_DATE+","+quantity+","+price+","+t_sale+");";
			//System.out.println("aaaaaaaaaaaaaaaaa="+sql);
			
			
			String sql = "INSERT IGNORE  into metro_dw.supplier_dim_table values("
					+ "\""+sid+"\", "
					+ "\""+sname+"\");" ;
			
			
			
			 stmt.executeUpdate(sql);
			 
			 //System.out.println("aaaaaaaaaaaaaaaaa="+line++);
			 
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 	
 }
 
  
 
		 
	
 
 public void insertIntoWarehouse(saleFact f) throws ClassNotFoundException, SQLException
 {
 	
 	
		
		Statement stmt=null;
		try {
			stmt=conn3.createStatement();
			
			String price=Integer.toString(f.PRICE);
			String t_sale=Integer.toString(f.TOTAL_SALE);
			String quantity=Integer.toString(f.QUANTITY);
			
			//String sql="INSERT INTO fact_table(transaction_id, customer_id, product_id, store_id, supplier_id, time_id,quantity,price,sale)\r\n"
				//	+ "VALUES("+f.TRANSACTION_ID+","+f.CUSTOMER_ID+","+ f.PRODUCT_ID+","+f.STORE_ID+","+f.SUPPLIER_ID+","+f.T_DATE+","+quantity+","+price+","+t_sale+");";
			//System.out.println("aaaaaaaaaaaaaaaaa="+sql);
			
			
			String sql = "INSERT  into metro_dw.salesfact_table values("
					//+ "\""+f.TRANSACTION_ID+"\", "
					+ "\""+f.CUSTOMER_ID+"\", "
					+ "\""+f.PRODUCT_ID+"\", "
				    + "\""+f.SUPPLIER_ID+"\", "
					+ "\""+f.STORE_ID+"\", "
					+ "\""+f.T_DATE+"\", "
					+ "\""+quantity+"\", "
					+ "\""+t_sale+"\");" ;
			
			
			
			 stmt.executeUpdate(sql);
			 
			 //System.out.println("aaaaaaaaaaaaaaaaa="+line++);
			 
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
 }
 
 public void dimTime(Date tempFact )
 {
 	Statement stmt=null;
		try {
			stmt=conn3.createStatement();
			String sql =  "INSERT IGNORE into time_dim_table values("
					+"\""+ tempFact + "\", "
					+ "day(\"" +tempFact+ "\"), "
					+ "month(\"" +tempFact+ "\"), "
					+ "quarter(\"" +tempFact+ "\"), "
					+ "year(\"" +tempFact+ "\") "
					+ ");" ;
			
			 stmt.executeUpdate(sql);
			 
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 	
 }
 
 
 public void removeQueueEnteries(String match)
 {
 	int count=0;
 	//System.out.println("removing ptoduct id :-"+match+"   current size="+queueConnection.size() );
 	for(int a=0;a<queueConnection.size();a++)
 	{
 		if(queueConnection.get(a).equals(match))
 		{
 			queueConnection.remove(a);
 			count++;
 		}
 	}
 	//System.out.println("total enteries removed :-"+count );
 }
 
 
 
 public void hybridAlgo() throws ClassNotFoundException, SQLException
 {
 	int chunkSize=10;
 	boolean check=true;
 	int size =queueConnection.size();
 	// System.out.println("number of tuples 5677in queue are:-"+queueConnection.size());
 	while(check==true)
 	{
 		int tempSize=queueConnection.size()-1;
 	//	System.out.println("current fetched sized:-"+tempSize);
 		String tempQueue= queueConnection.get(tempSize);
 	//	System.out.println("comparing product id:-"+tempQueue+"     tempSize="+tempSize);
 		
 		masterData metaFound=foundMatch(tempQueue);
 		
 	//	System.out.println("------------- master data object fetched and next 10 tuples brought into buffer");
 	/*	System.out.println("PRODUCT_ID:-"+metaFound.PRODUCT_ID);
 		System.out.println("PRODUCT_Name:-"+metaFound.PRODUCT_NAME);
 		System.out.println("Supplier_ID:-"+metaFound.SUPPLIER_ID);
 		System.out.println("comparing product id:-"+tempQueue);
 		System.out.println("retreiving data from the map");*/
 		
 		
 		
 		makeOutputTuple(tempQueue,metaFound);
 		//System.out.println("before size="+queueConnection.size());
 		removeQueueEnteries(tempQueue);
 		myMap.asMap().remove(tempQueue);
 		//System.out.println("after size="+queueConnection.size());
 		
 		if(queueConnection.size()<1)
 		{
 			check=false;
 		}
 		
 		
 	//	check=false;
 		
 	}
 }
	
 
 
 
 
	
 
 
 public void  makeOutputTuple(String key,masterData masterData) throws ClassNotFoundException, SQLException
 {
 	ArrayList<saleFact> tempFact=new ArrayList<saleFact>();
 	
 	for (String firstName : myMap.keySet()) {
 		
 		
 		if(firstName.equals(key))
 		{
 			//System.out.println("match has been found successfully in map");
 			Collection<store> lastNames = myMap.get(firstName);
 			Iterator<store> iterator = lastNames.iterator();
 			 
 	        // while loop
 	        while (iterator.hasNext()) {
 	        
 	        saleFact temp=new saleFact();
 	        store s=iterator.next();
 	        temp.CUSTOMER_ID=s.CUSTOMER_ID;
 	        temp.TRANSACTION_ID=s.TRANSACTION_ID;
 	        temp.CUSTOMER_NAME=s.CUSTOMER_NAME;
 	        temp.PRODUCT_ID=s.PRODUCT_ID;
 	        temp.STORE_ID=s.STORE_ID;
 	        temp.STORE_NAME=s.STORE_NAME;
 	        temp.T_DATE=s.T_DATE;
 	        temp.QUANTITY=s.QUANTITY;
 	        temp.PRICE=masterData.PRICE;
 	        temp.PRODUCT_NAME=masterData.PRODUCT_NAME;
 	        temp.SUPPLIER_ID=masterData.SUPPLIER_ID;
 	        temp.SUPPLIER_NAME=masterData.SUPPLIER_NAME;
 	        temp.TOTAL_SALE=masterData.PRICE*s.QUANTITY;
 	        
 	        tempFact.add(temp);
 	        
 	        fnode.add(temp);
 	        
 	        
 	       
 	        
 	        
 	        
 	        }
 		}
 		
 		// System.out.println("no of tuples created= " + tempFact.size());
         
     }
 	
 	
 	
 	//System.out.println("PRODUCT_ID   PRODUCT_NAME   PRICE   "+"match found compare="+tempFact.size());
 	
 //	System.out.println("PRODUCT_ID instances found:-  "+tempFact.get(0).PRODUCT_ID+"------------>"+          tempFact.size()+"----------->"+fnode.size());
 	for(int a=0;a<tempFact.size();a++)
 	{
 		//////    integrity constraints check    ///////
 		if(!dim_product.contains(tempFact.get(a).PRODUCT_ID))
          {
 			dim_product.add(tempFact.get(a).PRODUCT_ID);
 			dimProduct(tempFact.get(a).PRODUCT_NAME,tempFact.get(a).PRODUCT_ID,tempFact.get(a).PRICE); 
 			
 			prod++;
          }
 		if(!dim_supplier.contains(tempFact.get(a).SUPPLIER_ID))
          {
			    dim_supplier.add(tempFact.get(a).SUPPLIER_ID);
			    dimSupplier(tempFact.get(a).SUPPLIER_NAME,tempFact.get(a).SUPPLIER_ID); 
			    //prod++; 
          } 
 		if(!dim_store.contains(tempFact.get(a).STORE_ID))
         {
			  dim_store.add(tempFact.get(a).STORE_ID);
			  dimStore(tempFact.get(a).STORE_NAME,tempFact.get(a).STORE_ID);
			    //prod++;
         } 
 		if(!dim_customer.contains(tempFact.get(a).CUSTOMER_ID))
         {
			    dim_customer.add(tempFact.get(a).CUSTOMER_ID);
			    dimCustomer(tempFact.get(a).CUSTOMER_NAME,tempFact.get(a).CUSTOMER_ID);
			    //prod++;
         } 
 		
 		dimTime(tempFact.get(a).T_DATE);
 		
 		
 		
 		
 		
 		
 		
 		
 		if(line<100)
 		{
 			//insertIntoWarehouse( tempFact.get(a));
 		}
 		insertIntoWarehouse( tempFact.get(a));
  	    System.out.println(Integer.toString(line++)+" ) "+tempFact.get(a).PRODUCT_ID+"      "+tempFact.get(a).SUPPLIER_ID+"       "+tempFact.get(a).PRODUCT_NAME+"    "+tempFact.get(a).QUANTITY+"     "+tempFact.get(a).PRICE+"     "+tempFact.get(a).TOTAL_SALE+"      "+key+"          ");
 	}
 	
 	
 }
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 

	
	
	
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, ParseException {
		HybridJoin app = new HybridJoin();
//		Multimap<String, String> myMap = HashMultimap.create();
//		multiMap m=new multiMap();
		 
///	myMap.put("hamza", "1");
//	myMap.put("tayyab", "5000");
//	System.out.println(myMap.get("hamza")); // prints either "[1, 5000]" or "[5000, 1]"
//	myMap = ArrayListMultimap.create();
	//myMap.put(key, 1);
	//myMap.put(key, 5000);
	//System.out.println(myMap.get(key)); // always prints "[1,
	     	       
		app.run();
		System.out.println("number of tuples in fnode are:-"+app.fnode.size());
}
	
	
	
	
	
	

}
