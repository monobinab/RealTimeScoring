package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class PidDivLnDao {
	public class DivLn{
		public DivLn(String div, String ln) {
			this.div = div;
			this.ln = ln;
		}
		String div;
		String ln;
		public String getDiv(){
			return div;
		}
		public String getLn(){
			return ln;
		}
	}
	static final Logger LOGGER = LoggerFactory
			.getLogger(PidDivLnDao.class);
	static DB db;
    DBCollection pidDivLnCollection;
    static {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			LOGGER.error("Unable to get DB connection", e);
		}
    }
    public PidDivLnDao(){
		pidDivLnCollection = db.getCollection("pidDivLn");
    }
    public DivLn getVariableFromTopic(String pid){
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.PDL_PID, pid);
		DBObject obj = pidDivLnCollection.findOne(query);
		if (obj!=null) {
		    return new DivLn(obj.get(MongoNameConstants.PDL_D).toString(),
		    		obj.get(MongoNameConstants.PDL_L).toString());
		}
		return null;
	}
}
