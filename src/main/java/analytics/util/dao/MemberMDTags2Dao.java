package analytics.util.dao;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.objects.Tag;
import analytics.util.objects.TagList;

import com.google.gson.Gson;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MemberMDTags2Dao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberMDTags2Dao.class);
	DBCollection memberMDTagsCollection;

	public MemberMDTags2Dao() {
		// super("server2");
		super();
		memberMDTagsCollection = db.getCollection("memberMdTagsWithDates");
		LOGGER.info("collection in tagMetadataDao: "
				+ memberMDTagsCollection.getFullName());
	}

	public List<String> getMemberMDTagsBuSubBuList(String l_id) {
		DBObject dbObj = memberMDTagsCollection.findOne(new BasicDBObject(
				MongoNameConstants.L_ID, l_id));
		if (dbObj != null) {
			BasicDBList dbListTags = (BasicDBList) dbObj.get("tags");
			List<String> mdTags = new ArrayList<String>();
			for (Object tag : dbListTags) {
				if (tag instanceof String) {
					mdTags.add(tag.toString().substring(0, 5));
				}
			}
			// List<String> mdTags = (List<String>) dbObj.get("tags");
			return mdTags;
		} else
			return null;
	}

	public List<String> getMemberMDTags(String l_id) {
		BasicDBObject field = new BasicDBObject();
		field.put("tags.t", 1);
		DBObject dbObj = memberMDTagsCollection.findOne(new BasicDBObject(
				MongoNameConstants.L_ID, l_id), field);
		if (dbObj != null) {
			BasicDBList dbListTags = (BasicDBList) dbObj.get("tags");
			List<String> mdTags = new ArrayList<String>();
			if(dbListTags != null && dbListTags.size()>0){
				for (Object tag : dbListTags) {
					if (tag instanceof BasicDBObject) {
						mdTags.add(((BasicDBObject) tag).getString("t").toString());
					}
				}
				return mdTags;
				
			}
			return null;
		
		} else
			return null;
	}

	/*
	 * public void addMemberMDTags(String l_id, List<String> tags) { DBObject
	 * tagstoUpdate = new BasicDBObject();
	 * tagstoUpdate.put(MongoNameConstants.L_ID, l_id); BasicDBList mdTagsList =
	 * new BasicDBList(); for (String tag : tags) { mdTagsList.add(tag); }
	 * tagstoUpdate.put("tags", mdTagsList);
	 * LOGGER.info("tags are getting updated in " +
	 * memberMDTagsCollection.getDB().getName());
	 * memberMDTagsCollection.update(new BasicDBObject( MongoNameConstants.L_ID,
	 * l_id), tagstoUpdate, true, false); }
	 */
	public void deleteMemberMDTags(String l_id) {
		DBObject doc = memberMDTagsCollection.findOne(new BasicDBObject(
				MongoNameConstants.L_ID, l_id)); // get first document
		if (doc != null){
			BasicDBList rtsTagsList = (BasicDBList) doc.get("rtsTags");
			if(rtsTagsList == null || rtsTagsList.size() == 0)
				memberMDTagsCollection.remove(doc);
			else{
				DBObject tagstoUpdate = new BasicDBObject();
				tagstoUpdate.put("l_id", l_id);
				tagstoUpdate.put("rtsTags", rtsTagsList);
				
				LOGGER.info("Tags are getting updated in "
						+ memberMDTagsCollection.getDB().getName()
						+ " for memberId : '" + l_id + "'");
				memberMDTagsCollection.update(new BasicDBObject(
						MongoNameConstants.L_ID, l_id), tagstoUpdate, true, false);
			}
		}
	}

	/*
	 * public static void main(String[] args) { MemberMDTagsDao dao = new
	 * MemberMDTagsDao(); List<String> sample =
	 * dao.getMemberMDTags("hzuzVKVINbBBen+WGYQT/VJVdwI=");
	 * System.out.println(sample); }
	 */
	public void addMemberMDTags(String l_id, List<String> tags, HashMap<String, String> occasionDurationMap, 
			HashMap<String, String> occasionPriorityMap) throws ParseException {
		Date dNow = new Date();
		Date newDate = DateUtils.addMonths(new Date(), 6);
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.L_ID, l_id);
		DBObject doc = memberMDTagsCollection.findOne(query);
		BasicDBList mdTagsList = null;
		BasicDBList newMdTagsList = new BasicDBList();
		BasicDBObject newObj = null;
		BasicDBList rtsTagsList = new BasicDBList();
		// If there is a document already in the Collection for that Lid
		if (doc != null) {
			mdTagsList = (BasicDBList) doc.get("tags");
			rtsTagsList = (BasicDBList) doc.get("rtsTags");
			for (String tag : tags) {
				// Check if the Tag is already there in the document.
				// If yes, retain the effective and expiration dates. Else
				// create a new sub-document

				newObj = isTagExists(tag, mdTagsList);
				if (newObj == null) {
					newObj = new BasicDBObject();
					newObj.append("t", tag);
					newObj.append("f", ft.format(dNow));
										
				}
				//since the expiration date should always be updated, the e will be set out of the If condition
				newObj.append("e", ft.format(newDate));
				
				newMdTagsList.add(newObj);
			}
		}
		// If there is NO document in the Collection for that Lid
		else {
			for (String tag : tags) {
				newObj = new BasicDBObject();
				newObj.append("t", tag);
				newObj.append("f", ft.format(dNow));
				newObj.append("e", ft.format(newDate));
				newMdTagsList.add(newObj);
			}
		}
		
		//Perform Re-organization
		TagList tagList = reOrganizeAllTags(newMdTagsList,rtsTagsList,occasionPriorityMap);
		
		
		DBObject tagstoUpdate = new BasicDBObject();
		tagstoUpdate.put("l_id", l_id);
		
		if (tagList.getTags() != null && tagList.getTags().size() > 0)
		{
			tagstoUpdate.put("tags", tagList.getTags());
			LOGGER.info("PERSIST:mdTags are getting updated in "
				+ memberMDTagsCollection.getDB().getName()
				+ " for memberId : '" + l_id + "'");
		}
		if (tagList.getRtsTags() != null && tagList.getRtsTags().size() > 0)
		{
			tagstoUpdate.put("rtsTags", tagList.getRtsTags());
			LOGGER.info("PERSIST:rtsTags are getting updated in "
				+ memberMDTagsCollection.getDB().getName()
				+ " for memberId : '" + l_id +"'");
		}
		memberMDTagsCollection.update(new BasicDBObject(
				MongoNameConstants.L_ID, l_id), tagstoUpdate, true, false);
	}

	private BasicDBObject isTagExists(String tag, BasicDBList tagsList) {
		BasicDBObject obj = null;
		if (tagsList != null && tagsList.size() > 0) {
			for (Object tagObj : tagsList) {
				BasicDBObject obj1 = (BasicDBObject) tagObj;
				if (obj1.containsValue(tag)) {
					return obj1;
				}
			}
		}
		return obj;
	}

	public TagList reOrganizeAllTags(BasicDBList tags, BasicDBList rtsTags, HashMap<String,String> occasionPriorityMap) throws ParseException{
		
		BasicDBList unexpiredTags = null;
		BasicDBList unexpiredRtsTags = null;
		
		unexpiredTags = getUnExpiredTagsList(tags);
		unexpiredRtsTags = getUnExpiredTagsList(rtsTags);
		
		//retain the tags
		TagList tagList = determineTagsPriority(unexpiredTags, unexpiredRtsTags, occasionPriorityMap);
		
		return tagList;
	}

	private BasicDBList getUnExpiredTagsList(BasicDBList tags) 
			throws ParseException {
		
		BasicDBList unexpiredTags = null;
		
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
		Date dNow = new Date();
		String dateStr = ft.format(dNow);
		Date today = ft.parse(dateStr);
		Gson gson = new Gson();
		
		if(tags == null)
			return null;
		
		for( Object tagObj : tags){
			BasicDBObject obj1 = (BasicDBObject) tagObj;
			//Tag tag = mongoTemplate.getConverter().read(Tag.class, obj1);
			Tag tag = gson.fromJson(obj1.toString(), Tag.class);
			Date expDate = ft.parse((String) obj1.get("e"));
			if(expDate.after(today) || expDate.equals(today)){
				
				if(unexpiredTags==null)
					unexpiredTags = new BasicDBList();
				
				unexpiredTags.add(obj1);
			}
		}
		return unexpiredTags;
	}
	
	
	private TagList determineTagsPriority(BasicDBList tags, BasicDBList rtsTags, HashMap<String,String> occasionPriorityMap){
		
		TagList tagList = new TagList();
		BasicDBList retainedTags = new BasicDBList();
		BasicDBList retainedRtsTags = new BasicDBList();

		retainedTags = retainPriorityBasedBuSubBuWithinList(tags,occasionPriorityMap);
		retainedRtsTags = retainPriorityBasedBuSubBuWithinList(rtsTags,occasionPriorityMap);
		
		if((retainedTags==null || retainedTags.size()==0) && (retainedRtsTags==null || retainedRtsTags.size()==0)){
			return null;
		}
		else if(retainedRtsTags==null || retainedRtsTags.size()==0){
			tagList.setTags(retainedTags);
			return tagList;
		}
		else if(retainedTags==null || retainedTags.size()==0){
			tagList.setRtsTags(retainedRtsTags);
			return tagList;
		}
		
		/*retainPriorityBasedBuSubBu(tags, tags,occasionPriorityMap);
		retainPriorityBasedBuSubBu(rtsTags, rtsTags,occasionPriorityMap);*/
		
		/*retainedTags = retainPriorityBuSubBu(tags, occasionPriorityMap);
		retainedRtsTags = retainPriorityBuSubBu(rtsTags, occasionPriorityMap);*/
		
		retainPriorityBasedBuSubBu(retainedTags, retainedRtsTags,occasionPriorityMap);
		
		tagList.setTags(retainedTags);
		tagList.setRtsTags(retainedRtsTags);
		
		return tagList;
	}

	/**
	 * @param retainedTags
	 * @param retainedRtsTags
	 * @param gson
	 */
	private BasicDBList retainPriorityBasedBuSubBuWithinList(BasicDBList tags, HashMap<String, String> occasionPriorityMap) {
		
		if(tags == null)
			return null;
		
		Gson gson = new Gson();
		BasicDBList tagsWithinList = new BasicDBList();
		
		HashMap<String, TreeMap<Integer, DBObject>> priorityTagsMap = new HashMap<String, TreeMap<Integer, DBObject>>();
		
		for(Object obj : tags){
			Tag tag = gson.fromJson(((DBObject) obj).toString(), Tag.class);
			
			if(priorityTagsMap.size()==0 || priorityTagsMap.get(tag.getT().substring(0, 5)) == null){
				TreeMap<Integer, DBObject> priorityAndTag = new TreeMap<Integer, DBObject>();
				priorityAndTag.put(new Integer(occasionPriorityMap.get(tag.getT().substring(5, 6))), (DBObject)obj);
				priorityTagsMap.put(tag.getT().substring(0, 5), priorityAndTag);
			}
			else{
				TreeMap<Integer, DBObject> priorityAndTag = priorityTagsMap.get(tag.getT().substring(0, 5));
				priorityAndTag.put(new Integer(occasionPriorityMap.get(tag.getT().substring(5, 6))), (DBObject)obj);
				priorityTagsMap.put(tag.getT().substring(0, 5), priorityAndTag);
			}
			
		}
		
		Set<String> tagsSet = priorityTagsMap.keySet();
		Iterator<String> iter = tagsSet.iterator();
		while(iter.hasNext()){
			String tag = iter.next();
			Integer key = priorityTagsMap.get(tag).firstKey();
			tagsWithinList.add((DBObject)priorityTagsMap.get(tag).get(key));
		}
		
		return tagsWithinList;
	}
	
	
	public void retainPriorityBasedBuSubBu(BasicDBList tags, BasicDBList rtsTags, HashMap<String, String> occasionPriorityMap){
		
		Gson gson = new Gson();
		Iterator<Object> rtsIter = rtsTags.iterator();
		while(rtsIter.hasNext()){
			Tag tag1 =  gson.fromJson(((DBObject) rtsIter.next()).toString(), Tag.class);
			Iterator<Object> tagIter = tags.iterator();
			
			while(tagIter.hasNext()){
				
				Tag tag2 = gson.fromJson(((DBObject) tagIter.next()).toString(), Tag.class);
				
				if(tag1.getT().substring(0, 5).equalsIgnoreCase(tag2.getT().substring(0, 5))){
					
					if(occasionPriorityMap.get(tag1.getT().substring(5, 6)).equalsIgnoreCase(occasionPriorityMap.get(tag2.getT().substring(5, 6))))
						tagIter.remove();
					
					else if(new Integer(occasionPriorityMap.get(tag1.getT().substring(5, 6))) > 
										(new Integer (occasionPriorityMap.get(tag2.getT().substring(5, 6)))))	
						rtsIter.remove();
					
					else if(new Integer(occasionPriorityMap.get(tag1.getT().substring(5, 6))) < 
										(new Integer (occasionPriorityMap.get(tag2.getT().substring(5, 6)))))
						tagIter.remove();
				}
			}
		}
	}
	
	
	/*public void addRtsMemberTags(String l_id, List<String> tags) throws ParseException{
		addRtsMemberTags(l_id, tags, null, null);
	}*/
	
	
	public void addRtsMemberTags(String l_id, List<String> tags, HashMap<String, String> occasionDurationMap, 
			HashMap<String, String> occasionPriorityMap) throws ParseException {
		Date dNow = new Date();
		//Date tomorrow = new Date(dNow.getTime() + (1000 * 60 * 60 * 24));
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.L_ID, l_id);
		DBObject doc = memberMDTagsCollection.findOne(new BasicDBObject(
				MongoNameConstants.L_ID, l_id));
		BasicDBList rtsTagsList = null;
		BasicDBList newRtsTagsList = new BasicDBList();
		BasicDBList mdTagsList = new BasicDBList();
		BasicDBObject newObj = null;
		// If there is a document already in the Collection for that Lid
		if (doc != null) {
			rtsTagsList = (BasicDBList) doc.get("rtsTags");
			mdTagsList = (BasicDBList) doc.get("tags");
			for (String tag : tags) {
				// Check if the Tag is already there in the document.
				// If yes, retain the effective and expiration dates. Else
				// create a new sub-document
				newObj = isTagExists(tag, rtsTagsList);
				if (newObj == null) {
					newObj = new BasicDBObject();
					newObj.append("t", tag);
					newObj.append("f", ft.format(dNow));
					newObj.append("e", ft.format(getExpirationDate(tag,occasionDurationMap)));
					
					if (rtsTagsList == null)
						rtsTagsList = new BasicDBList();
					
					rtsTagsList.add(newObj);
				}
				else{
					newObj.append("f", ft.format(dNow));
					newObj.append("e", ft.format(getExpirationDate(tag,occasionDurationMap)));
				}
				
			}
		}
		// If there is NO document in the Collection for that Lid
		else {
			rtsTagsList = new BasicDBList();
			for (String tag : tags) {
				newObj = new BasicDBObject();
				newObj.append("t", tag);
				newObj.append("f", ft.format(dNow));
				newObj.append("e", ft.format(getExpirationDate(tag,occasionDurationMap)));
				rtsTagsList.add(newObj);
			}
		}
		
		//Perform Re-organization
		TagList tagList = reOrganizeAllTags(mdTagsList,rtsTagsList,occasionPriorityMap);
		
		DBObject tagstoUpdate = new BasicDBObject();
		tagstoUpdate.put("l_id", l_id);
		
		if (tagList.getTags() != null && tagList.getTags().size() > 0)
			tagstoUpdate.put("tags", tagList.getTags());
		if (tagList.getRtsTags() != null && tagList.getRtsTags().size() > 0)
			tagstoUpdate.put("rtsTags", tagList.getRtsTags());
		
		LOGGER.info("rtsTags are getting updated in "
				+ memberMDTagsCollection.getDB().getName()
				+ " for memberId : '" + l_id + "'");
		memberMDTagsCollection.update(new BasicDBObject(
				MongoNameConstants.L_ID, l_id), tagstoUpdate, true, false);
	}
	

	private Date getExpirationDate(String tag, HashMap<String, String> occasionDurationMap){
		
		Integer days = 1;
		if(occasionDurationMap!=null && occasionDurationMap.get(tag.substring(5, 6))!=null)
				days = new Integer (occasionDurationMap.get(tag.substring(5, 6)));
		
		Date date = DateUtils.addDays(new Date(), days);
		
		return date;
	}

	public void deleteMemberMDTag(String l_id, String mdtag) {
		DBObject doc = memberMDTagsCollection.findOne(new BasicDBObject(
				MongoNameConstants.L_ID, l_id));
		if (doc != null) {
			if (doc.containsField("rtsTags")) {
				List<String> rtsTagsToBeRemoved = new ArrayList<String>();
				rtsTagsToBeRemoved.add(mdtag);
				updateTags(l_id, rtsTagsToBeRemoved, doc, "rtsTags");
			}

			else if (doc.containsField("tags")) {
				List<String> tagsToBeRemoved = new ArrayList<String>();
				tagsToBeRemoved.add(mdtag);
				updateTags(l_id, tagsToBeRemoved, doc, "tags");
			}
		}
	}

	/**
	 * @param l_id
	 * @param mdtag
	 * @param doc
	 */
	private void updateTags(String l_id, List<String> tagsToBeRemoved,
			DBObject doc, String docName) {
		BasicDBList rtsTagsList;
		BasicDBObject tagObjToBeDeleted;
		// rtsTagsList = (BasicDBList) doc.get("rtsTags");
		rtsTagsList = (BasicDBList) doc.get(docName);
		for (String tagToBeRemoved : tagsToBeRemoved) {
			tagObjToBeDeleted = isTagExists(tagToBeRemoved, rtsTagsList);
			if (tagObjToBeDeleted != null) {

				rtsTagsList.remove(tagObjToBeDeleted);
			}
		}

		DBObject tagstoUpdate = new BasicDBObject();
		tagstoUpdate.put("l_id", l_id);
		tagstoUpdate.put(docName, rtsTagsList);
		LOGGER.info("tags are getting updated in "
				+ memberMDTagsCollection.getDB().getName() + "for memberId : '"
				+ l_id + "'");
		memberMDTagsCollection.update(new BasicDBObject(
				MongoNameConstants.L_ID, l_id), tagstoUpdate, true, false);
	}

	public void deleteMemberMDTags(String l_id, List<String> inactiveTop5TagsLst) {
		DBObject doc = memberMDTagsCollection.findOne(new BasicDBObject(
				MongoNameConstants.L_ID, l_id));
		if (doc != null) {
			if (doc.containsField("rtsTags")) {

				updateTags(l_id, inactiveTop5TagsLst, doc, "rtsTags");
			}

			else if (doc.containsField("tags")) {

				updateTags(l_id, inactiveTop5TagsLst, doc, "tags");
			}
		}
	}
	

}
