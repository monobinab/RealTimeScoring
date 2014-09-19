package analytics.util;

import com.github.fakemongo.Fongo;
import com.mongodb.DB;

public class FakeMongo {
	private static DB conn;
	public static DB getTestDB() {
		if(conn == null)
			new FakeMongo();
		return conn;
	}
	
	private FakeMongo(){
		Fongo fongo = new Fongo("test server");
		conn = fongo.getDB("test");
	}
	
	public static void setDBConn(DB connection){
		conn = connection;
	}
	
}
