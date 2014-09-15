package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class DivCatKsnDao {
	public class DivCat{
		public DivCat(String div, String cat) {
			this.div = div;
			this.cat = cat;
		}
		String div;
		String cat;
		public String getDiv(){
			return div;
		}
		public String getCat(){
			return cat;
		}
	}
	static final Logger LOGGER = LoggerFactory
			.getLogger(DivCatKsnDao.class);
	static DB db;
    DBCollection divCatKsnCollection;
    static {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			LOGGER.error("Unable to get DB connection", e);
		}
    }
    public DivCatKsnDao(){
		divCatKsnCollection = db.getCollection("divCatKsn");
    }
    public DivCat getVariableFromTopic(String ksn){
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.DCK_K, ksn);
		DBObject obj = divCatKsnCollection.findOne(query);
		if (obj!=null) {
		    return new DivCat(obj.get(MongoNameConstants.DCK_D).toString(),
		    		obj.get(MongoNameConstants.DCK_C).toString());
		}
		return null;
	}
}
