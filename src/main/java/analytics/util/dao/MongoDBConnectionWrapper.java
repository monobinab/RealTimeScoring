package analytics.util.dao;

import com.mongodb.DB;

public class MongoDBConnectionWrapper {

	private static MongoDBConnectionWrapper mongoDBConnectionWrapper;
	
	public DB db1 = null;
	public DB db2 = null;
	public DB db3 = null;
	private MongoDBConnectionWrapper(){
		
	}
	
	public static MongoDBConnectionWrapper getInstance(){
        if(mongoDBConnectionWrapper == null){
        	mongoDBConnectionWrapper = new MongoDBConnectionWrapper();
        }
        return mongoDBConnectionWrapper;
    }
	
	public void populateDBConnection(DB db1, DB db2){
		this.db1 = db1;
		this.db2 = db2;
	}
}
