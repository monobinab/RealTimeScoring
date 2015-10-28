package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class TraitBuSubBuDao extends AbstractDao{
	
	private static final Logger LOGGER = LoggerFactory
			.getLogger(TraitBuSubBuDao.class);
	DBCollection traitBuSubBuColl;

	public TraitBuSubBuDao() {
		traitBuSubBuColl = db.getCollection("traitBuSubBu");
	}
	
	public Map<String, String> getTraitBuSubBuMap(){
		DBCursor cursor = traitBuSubBuColl.find();
		Map<String, String> traitBuSubBuMap = new HashMap<String, String>();
		while(cursor.hasNext()){
			DBObject dbObj = cursor.next();
				traitBuSubBuMap.put(dbObj.get(MongoNameConstants.TRAIT_ID).toString(), dbObj.get(MongoNameConstants.BU_SUBBU).toString());
		}
		return traitBuSubBuMap;
	}
}
