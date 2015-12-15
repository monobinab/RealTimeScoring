package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import analytics.util.MongoNameConstants;
import analytics.util.objects.BoostBrowseBuSubBu;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class BoostBrowseBuSubBuDao extends AbstractDao{
	
	 DBCollection boostBrowseBuSubBuCollection;
	    public BoostBrowseBuSubBuDao(){
	    	super();
	    	boostBrowseBuSubBuCollection = db.getCollection("boostBrowseBuSubBu");
	    }
		
		public Map<String, BoostBrowseBuSubBu> getBoostBuSubBuFromModelCode(){
			Map<String, BoostBrowseBuSubBu> modelCodeToBoostBuSubBuMap = new HashMap<String, BoostBrowseBuSubBu>();
			DBCursor vCursor = boostBrowseBuSubBuCollection.find();
			for (DBObject dbObj: vCursor) {
				BoostBrowseBuSubBu boostBrowseBuSubBu = new BoostBrowseBuSubBu();
				boostBrowseBuSubBu.setBoost((String)dbObj.get(MongoNameConstants.BOOST));//BU_SUBBU
				boostBrowseBuSubBu.setBuSubBu((String)dbObj.get(MongoNameConstants.BU_SUBBU));
				modelCodeToBoostBuSubBuMap.put((String)dbObj.get(MongoNameConstants.MODEL_CODE), boostBrowseBuSubBu);
			}
			return modelCodeToBoostBuSubBuMap;
		}

	}