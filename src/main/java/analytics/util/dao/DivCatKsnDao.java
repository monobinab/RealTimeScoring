package analytics.util.dao;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.objects.DivCatLineItem;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class DivCatKsnDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DivCatKsnDao.class);
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
	
    DBCollection divCatKsnCollection;
    public DivCatKsnDao(){
    	super();
    	if(db != null){
    		divCatKsnCollection = db.getCollection("divCatKsn");
    	}
    	else{
    		LOGGER.error("db NULL in DivCatKsnDa0");
    	}
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
    
    public List<DivCatLineItem> getDivCatItemFromItems(List<String> ksn){
    	BasicDBObject query = new BasicDBObject();
      	List<DivCatLineItem> divCatList = new ArrayList<DivCatLineItem>();
		query.put(MongoNameConstants.DCK_K, new BasicDBObject("$in", ksn));
		DBCursor cursor = divCatKsnCollection.find(query);
		if(divCatKsnCollection != null){
			if(cursor != null){
				while(cursor.hasNext()){
					DBObject obj = cursor.next();
					if(obj != null){
						DivCatLineItem divCatItem = new DivCatLineItem();
						divCatItem.setDiv((obj.get(MongoNameConstants.DCK_D).toString()));
						divCatItem.setCat((obj.get(MongoNameConstants.DCK_C).toString()));
						divCatItem.setItem((obj.get(MongoNameConstants.DCK_K).toString()));
						divCatList.add(divCatItem);
					}
				}
			}
		}
		  return divCatList;
	}
}
