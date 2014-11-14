package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class DCDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory.getLogger(PidDivLnDao.class);
	DBCollection dcQAStrengths;

	public DCDao() {
		super();
		dcQAStrengths = db.getCollection(MongoNameConstants.DC_QA_STRENGTHS); // MongoNameConstants.PID_DIV_LN_COLLECTION
	}

	public Object getStrength(String category, String question_id, String answer_id) {
		if (category.equals("DC_Appliances")) {
			BasicDBObject query = new BasicDBObject();
			query.put("q", question_id);
			query.put("a", answer_id);
			query.put("c", category);
			if(dcQAStrengths != null){
				DBObject obj = dcQAStrengths.findOne(query);
				if (obj != null) {
					return obj; //obj.get("strength");
				}
			}
			else{
				LOGGER.debug("Mongo Fetch Failure at DC_QA_STRENGTHS");
			}
		}
		return null;
	}

}
