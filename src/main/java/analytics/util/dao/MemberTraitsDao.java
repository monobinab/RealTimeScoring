package analytics.util.dao;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;


public class MemberTraitsDao extends AbstractDao{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberTraitsDao.class);
    DBCollection memberTraitsCollection;
    public MemberTraitsDao(){
    	super();
		memberTraitsCollection = db.getCollection("memberTraits");
    }
    
    
    public Map<String, List<String>> getDateTraits(String l_id) {
    	Map<String, List<String>> dateTraitsMap  = new HashMap<String,List<String>>();
		DBObject memberTraitsDBO = memberTraitsCollection.findOne(new BasicDBObject().append("l_id", l_id));
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

		if(memberTraitsDBO != null && memberTraitsDBO.keySet().contains(l_id)) {
			
			BasicDBList dates = (BasicDBList) memberTraitsDBO.get("date");
			
			for( Iterator<Object> dateIterator = dates.iterator(); dateIterator.hasNext(); ) {
				BasicDBObject dateDBO = (BasicDBObject) dateIterator.next();
				try {
					if(simpleDateFormat.parse(dateDBO.get("d").toString()).after(new Date(new Date().getTime() + (-7 * 1000 * 60 * 60 * 24)))) {
						List<String> newTraitsCollection = new ArrayList<String>();
						dateTraitsMap.put(dateDBO.get("d").toString(), newTraitsCollection);
						BasicDBList traitsDBList = (BasicDBList) dateDBO.get("t");
						if(traitsDBList != null && !traitsDBList.isEmpty()) {
							for( Iterator<Object> tIterator = traitsDBList.iterator(); tIterator.hasNext(); ) {
								Object t = tIterator.next();
								dateTraitsMap.get(dateDBO.get("d").toString()).add(t.toString());
							}
						}
					}
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
		return dateTraitsMap;
	}


	public void addDateTrait(String l_id, Map<String,List<String>> dateTraitMap) {
		BasicDBList dateTraitList = new BasicDBList();
		for(String date : dateTraitMap.keySet()){
			dateTraitList.add(new BasicDBObject("d", date).append("t", dateTraitMap.get(date)));
		}		
		DBObject objectToInsert = new BasicDBObject();
		objectToInsert.put("l_id", l_id);
		objectToInsert.put("date", dateTraitList);
		memberTraitsCollection.update(new BasicDBObject("l_id", l_id), objectToInsert, true, false);
	}
}

	