package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class PidDivLnDao extends AbstractDao{
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
		public String getDivLn(){
			return ln;
		}

        public String toString()
        {
            return " div = "+ div + ", ln = "+ln;
        }

	}
	private static final Logger LOGGER = LoggerFactory
			.getLogger(PidDivLnDao.class);
    DBCollection pidDivLnCollection;
    public PidDivLnDao(){
    	super();
		pidDivLnCollection = db.getCollection(MongoNameConstants.PID_DIV_LN_COLLECTION);
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
