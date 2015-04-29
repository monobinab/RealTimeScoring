package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.Constants;
import analytics.util.MongoNameConstants;
import analytics.util.objects.TagVariable;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class TagVariableDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(TraitVariablesDao.class);
	DBCollection tagVariablesCollection;

	public TagVariableDao() {
		super();
		tagVariablesCollection = db.getCollection("tagVariable");
	}
	
	public Map<String, TagVariable> getTagVariables(){
		Map<String, TagVariable> tagVariablesMap = new HashMap<String, TagVariable>();
		DBCursor tagVariables = tagVariablesCollection.find();
		while(tagVariables.hasNext()){
			DBObject tagVariableObject = tagVariables.next();
			if(tagVariableObject!=null)
			{
				tagVariablesMap.put(tagVariableObject.get(MongoNameConstants.TAG_VAR_MDTAG).toString(), 
						new TagVariable(tagVariableObject.get(MongoNameConstants.TAG_VAR_MDTAG).toString(),
								tagVariableObject.get(MongoNameConstants.TAG_VAR_MODEL).toString(),
										tagVariableObject.get(MongoNameConstants.TAG_VAR_VAR).toString()));
			}
		}
		return tagVariablesMap;
	}

	public Map<String, String> getTagVariable(String tag) {
		DBObject tagVariable = tagVariablesCollection
				.findOne(new BasicDBObject(MongoNameConstants.TAG_VAR_MDTAG,
						tag.substring(0, 5)));
		if (tagVariable != null
				&& tagVariable.containsField(MongoNameConstants.TAG_VAR_VAR)
				&& tagVariable.containsField(MongoNameConstants.TAG_VAR_MODEL)) {
			Map<String, String> tagVarMap = new HashMap<String, String>();
			tagVarMap.put(
					(String) tagVariable.get(MongoNameConstants.TAG_VAR_VAR),
					tagVariable.get(MongoNameConstants.TAG_VAR_MODEL) + "");
			return tagVarMap;
		} else {
			LOGGER.info("No Tag Variable for tag " + tag);
		}
		return null;
	}

	public List<String> getTagVariablesList(List<String> tagsList) {
		BasicDBList list = new BasicDBList();
		for (String tag : tagsList) {
			list.add(tag);
		}
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.TAG_VAR_MDTAG, new BasicDBObject("$in",
				list));
		DBCursor tagVariablesCursor = tagVariablesCollection.find(query);
		List<String> tagVariablesList = new ArrayList<String>();
		while (tagVariablesCursor.hasNext()) {
			tagVariablesList.add((String) tagVariablesCursor.next().get(
					MongoNameConstants.TAG_VAR_VAR));
		}
		return tagVariablesList;
	}

	public Map<Integer, String> getTagModelIds(List<String> tagsList) {
		Map<Integer, String> tagModelMap = new HashMap<Integer, String>();
		for (String tag : tagsList) {
			BasicDBObject query = new BasicDBObject();
			query.put(MongoNameConstants.TAG_VAR_MDTAG, tag);
			DBObject dbObj = tagVariablesCollection.findOne(query);
			if (dbObj != null) {
				tagModelMap.put(
						(Integer) dbObj.get(MongoNameConstants.TAG_VAR_MODEL),
						"" + dbObj.get(MongoNameConstants.TAG_VAR_MDTAG));
			}
		}
		/*
		 * BasicDBObject query = new BasicDBObject();
		 * query.put(MongoNameConstants.TAG_VAR_MDTAG, new BasicDBObject("$in",
		 * list)); DBCursor tagModelsCursor =
		 * tagVariablesCollection.find(query); //List<String> tagModelsList =
		 * new ArrayList<String>(); //Map<Integer, String> tagModelMap = new
		 * HashMap<Integer, String>(); while(tagModelsCursor.hasNext()){
		 * //tagModelsList.add("" +
		 * tagModelsCursor.next().get(MongoNameConstants.TAG_VAR_MODEL));
		 * tagModelMap.put((Integer)
		 * tagModelsCursor.next().get(MongoNameConstants.TAG_VAR_MODEL), "" +
		 * tagModelsCursor.next().get(MongoNameConstants.TAG_VAR_MDTAG)); }
		 */
		return tagModelMap;
	}
}
