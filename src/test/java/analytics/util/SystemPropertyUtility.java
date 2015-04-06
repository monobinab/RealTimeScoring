package analytics.util;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;

import com.github.fakemongo.Fongo;
import com.mongodb.DB;

public class SystemPropertyUtility {
	protected static DB db;
	protected static Map<String, String> stormConf;
	
	public static DB getDb() {
		return db;
	}
	public static void setDb(DB db) {
		SystemPropertyUtility.db = db;
	}
	public static Map<String, String> getStormConf() {
		return stormConf;
	}
	public static void setStormConf(Map<String, String> stormConf) {
		SystemPropertyUtility.stormConf = stormConf;
	}
	public static void setSystemProperty() {
		System.setProperty("rtseprod", "test");
		stormConf = new HashMap<String, String>();
		stormConf.put("nimbus.host", "test");
		FakeMongo.setDBConn(new Fongo("test db").getDB("test"));	
		try {
			db = DBConnection.getDBConnection();
			setDb(db);
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
		
		}
	public static void dropDatabase(){
		if(db.toString().equalsIgnoreCase("FongoDB.test"))
			db.dropDatabase();
		  else
		   Assert.fail("Something went wrong. Tests connected to " + db.toString());
		}
	}