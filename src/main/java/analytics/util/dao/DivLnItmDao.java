package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class DivLnItmDao {
	static final Logger LOGGER = LoggerFactory
			.getLogger(DivLnItmDao.class);
	static DB db;
    DBCollection divLnItemCollection;
    static {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			LOGGER.error("Unable to get DB connection", e);
		}
    }
    public DivLnItmDao(){
		divLnItemCollection = db.getCollection("divLnItm");
    }
    
    public String getLnFromDivItem(String div, String item) {
		//System.out.println("searching for line");
		
		BasicDBObject queryLine = new BasicDBObject();
		queryLine.put(MongoNameConstants.DLI_DIV, div);
		queryLine.put(MongoNameConstants.DLI_ITEM, item);
		
		//System.out.println("query: " + queryLine);
		DBObject divLnItm = divLnItemCollection.findOne(queryLine);
		//System.out.println("line: " + divLnItm);
		
		if(divLnItm==null || divLnItm.keySet()==null || divLnItm.keySet().isEmpty()) {
			return null;
		}
		String line = divLnItm.get(MongoNameConstants.DLI_LN).toString();
		//System.out.println("  found line: " + line);
		return line;
	}
}
