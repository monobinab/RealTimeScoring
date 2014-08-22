package analytics.util;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.MongoClient;

public class MongoUtils {
	private static DB db;
    private static MongoClient mongoClient;

	public static String getBoostVariable(String div, String line){
		return "BOOST_HA_WH_SYW";
	}
	
	//TODO: Read values from a property file
	public static DB getClient(String env) throws UnknownHostException{
		if(env.equals("PROD")){
			mongoClient = new MongoClient("trprmongod1.vm.itg.corp.us.shldcorp.com", 27000);
			db = mongoClient.getDB("test");
		    db.authenticate("appuser", "sears123".toCharArray());
		}
		else if(env.equals("QA")){
			mongoClient = new MongoClient("shrdmdb301p.stag.ch3.s.com", 20000);
			db = mongoClient.getDB("RealTimeScoring");
		    db.authenticate("rtsw", "5core123".toCharArray());
		}
		else{
			mongoClient = new MongoClient("trprrta2mong4.vm.itg.corp.us.shldcorp.com", 27000);
	          db = mongoClient.getDB("test");
		}
		return db;
	}
	
      
}
