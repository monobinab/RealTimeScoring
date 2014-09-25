
package analytics.util.dao;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MemberBoostsDao extends AbstractDao {
	public static void main(String[] args) {
		MemberBoostsDao dao = new MemberBoostsDao();
		Set<String> boostSet = new HashSet<String>();
		boostSet.add("BOOST_SYW_CE_LIKETCOUNT");
		boostSet.add("BOOST_SYW_XX_LIKETCOUNT");
		boostSet.add("BOOST_SYW_HA_LIKETCOUNT");
		dao.getMemberBoostsValues("dxo0b7SN1eER9shCSj0DX+eSGag=", boostSet);
	}

	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberVariablesDao.class);

	DBCollection memberBoostsCollection;

	public MemberBoostsDao() {
		super();
		memberBoostsCollection = db
				.getCollection(MongoNameConstants.MBR_BOOSTS_COLLECTION);
	}

	public Map<String, Map<String, List<String>>> getMemberBoostsValues(
			String l_id, Set<String> boostSet) {

		// Map of boost name to a map of date to a list of values
		Map<String, Map<String, List<String>>> mapToReturn = new HashMap<String, Map<String, List<String>>>();

		DBObject mbrBoostsDBO = memberBoostsCollection
				.findOne(new BasicDBObject(MongoNameConstants.L_ID, l_id));
		if (mbrBoostsDBO == null) {
			return null;
		}
		BasicDBList mbrBoostsList = (BasicDBList) mbrBoostsDBO
				.get(MongoNameConstants.BOOSTS_ARRAY);
		if (mbrBoostsList == null) {
			return null;
		}

		for (Object boostObj : mbrBoostsList) {
			for (String boost : ((DBObject) boostObj).keySet()) {
				if (!mapToReturn.containsKey(boost) && boostSet.contains(boost)) {
					mapToReturn.put((String) boost,	new HashMap<String, List<String>>());
					BasicDBList dateDBList = (BasicDBList) ((DBObject) boostObj)
							.get(boost);
					for (Object dateValue : dateDBList) {
						for (String date : ((DBObject) dateValue).keySet()) {
							if (!mapToReturn.get(boost).containsKey(date)) {
								mapToReturn.get(boost).put((String) date,new ArrayList<String>());
							}
							BasicDBList valuesDBList = (BasicDBList) ((DBObject)dateValue).get(date);
							for (Object value : valuesDBList) {
								mapToReturn.get(boost).get(date).add(value.toString());
							}
						}
					}
				}
			}
		}
		return mapToReturn;
	}
	
	public void writeMemberBoostValues(String l_id, Map<String, Map<String, List<String>>> memberBoostValuesMap) {
		for(String boost: memberBoostValuesMap.keySet()) {
			
		}
	}
}

