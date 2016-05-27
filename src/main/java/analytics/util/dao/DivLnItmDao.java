package analytics.util.dao;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.objects.DivCatLineItem;
import analytics.util.objects.DivLn;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class DivLnItmDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DivLnItmDao.class);
    DBCollection divLnItemCollection;
    public DivLnItmDao(){
    	super();
    	if(db != null){
    		divLnItemCollection = db.getCollection("divLnItm");
    	}
    	else{
    		LOGGER.error("db NULL in DivLnItmDao");
    	}
    }
    
    public String getLnFromDivItem(String div, String item) {
		BasicDBObject queryLine = new BasicDBObject();
		queryLine.put(MongoNameConstants.DLI_DIV, div);
		queryLine.put(MongoNameConstants.DLI_ITEM, item);
		DBObject divLnItm = divLnItemCollection.findOne(queryLine);
		if(divLnItm==null || divLnItm.keySet()==null || divLnItm.keySet().isEmpty()) {
			return null;
		}
		return divLnItm.get(MongoNameConstants.DLI_LN).toString();
	}
      
    public DivLn getLnFromDivItemTag(String div, String item) {
		
		BasicDBObject queryLine = new BasicDBObject();
		queryLine.put(MongoNameConstants.DLI_DIV, div);
		queryLine.put(MongoNameConstants.DLI_ITEM, item);
		DBObject divLnItm = divLnItemCollection.findOne(queryLine);
		if(divLnItm==null || divLnItm.keySet()==null || divLnItm.keySet().isEmpty()) {
			return null;
		}
		return new DivLn(div, div+divLnItm.get(MongoNameConstants.DLI_LN).toString(), divLnItm.get(MongoNameConstants.DLI_TAG).toString());
	}
    
    
    public List<DivCatLineItem> getDivCatLineFromDivItem(List<String> divItemsList){
       	BasicDBObject andQuery = new BasicDBObject();
    	List<DivCatLineItem> divLineItemList = new ArrayList<DivCatLineItem>();
    	List<BasicDBObject> queryList = new ArrayList<BasicDBObject>();
    	for(String divItem : divItemsList){
    		String[] str = divItem.split(",");
    		queryList.add(new BasicDBObject(MongoNameConstants.DLI_ITEM, str[0]).append(MongoNameConstants.DLI_DIV, str[1]));
    	}
    	andQuery.put("$or", queryList);
    	if(divLnItemCollection != null){
	    	DBCursor cursor = divLnItemCollection.find(andQuery);
	    	while(cursor.hasNext()){
	    		DBObject obj = cursor.next();
	    		if(obj != null){
					DivCatLineItem divLineItem = new DivCatLineItem();
					divLineItem.setDiv((obj.get(MongoNameConstants.DLI_DIV).toString()));
					divLineItem.setItem((obj.get(MongoNameConstants.DLI_ITEM).toString()));
					divLineItem.setLine(obj.get(MongoNameConstants.DLI_LN).toString());
					divLineItemList.add(divLineItem);
				}
	    	}
    	}
		return divLineItemList;
    }
}
