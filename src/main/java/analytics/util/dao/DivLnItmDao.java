package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class DivLnItmDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DivLnItmDao.class);
    DBCollection divLnItemCollection;
    public DivLnItmDao(){
    	super();
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
		return divLnItm.get(MongoNameConstants.DLI_LN).toString();
		//System.out.println("  found line: " + line);
	}
}
