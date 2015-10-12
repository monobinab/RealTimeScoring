package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.objects.DivLn;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class DivLnItmDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DivLnItmDao.class);
    DBCollection divLnItemCollection;
    public DivLnItmDao(){
    	super();
		divLnItemCollection = db.getCollection("divLnItmTest");
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
		//return new DivLn(divLnItm.get(MongoNameConstants.DLI_LN).toString(), divLnItm.get(MongoNameConstants.DLI_TAG).toString());
		return divLnItm.get(MongoNameConstants.DLI_LN).toString();
		//System.out.println("  found line: " + line);
	}
      
    public DivLn getLnFromDivItemTag(String div, String item) {
		
		BasicDBObject queryLine = new BasicDBObject();
		queryLine.put(MongoNameConstants.DLI_DIV, div);
		queryLine.put(MongoNameConstants.DLI_ITEM, item);
	
		DBObject divLnItm = divLnItemCollection.findOne(queryLine);
		//System.out.println("line: " + divLnItm);
		
		if(divLnItm==null || divLnItm.keySet()==null || divLnItm.keySet().isEmpty()) {
			return null;
		}
		return new DivLn(div, div+divLnItm.get(MongoNameConstants.DLI_LN).toString(), divLnItm.get(MongoNameConstants.DLI_TAG).toString());
	}
}
