package analytics.util.dao;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class DivLnItmDao {
	DB db;
    DBCollection divLnItemCollection;
    {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
