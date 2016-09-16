package analytics.util.dao;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import analytics.util.objects.KeyWordModelCode;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class IsKeyWordModelDao extends AbstractDao{

	 DBCollection isKeyWordModelCollection;
	 public IsKeyWordModelDao(){
		super();
		isKeyWordModelCollection = db.getCollection("isKeywordModel");
	 }
	 
/*	 @SuppressWarnings("unchecked")
	public List<String> getModelCodeFromSearchString(String searchTerm) throws ParseException{
		DBObject dbObj = isKeyWordModelCollection.findOne(new BasicDBObject("k", searchTerm));
		List<String> modelCodes = null;
		if(dbObj != null){
			Date todayDate = new Date();
			Calendar cal = Calendar.getInstance(); 
			cal.setTime(dtFormat.parse((String) dbObj.get("d")));
			cal.add(Calendar.MONTH, 6);
			Date sixMonthDate = cal.getTime();
			System.out.println(sixMonthDate);
			if(todayDate.before(sixMonthDate) || todayDate.equals(sixMonthDate)){
				modelCodes = (List<String>) dbObj.get("m");
			}
		}
			return modelCodes;
	 }*/
	 
	 @SuppressWarnings("unchecked")
	public KeyWordModelCode getModelCodeFromSearchString(String searchTerm) throws ParseException{
		DBObject dbObj = isKeyWordModelCollection.findOne(new BasicDBObject("k", searchTerm));
		KeyWordModelCode keyWordModelCode = null;
		if(dbObj != null){
			keyWordModelCode = new KeyWordModelCode();
			keyWordModelCode.setKeyWord((String)dbObj.get("k"));
			keyWordModelCode.setModelCodesList((List<String>) dbObj.get("m"));
			keyWordModelCode.setDate((String)dbObj.get("d"));
		}
			return keyWordModelCode;
	 }
	 
	 public void addModelCodesForSearchStrings(String searchString, Set<String> modelCodes, String date){
		 //DBObject dbObj = isKeyWordModelCollection.findOne(new BasicDBObject("k", searchString));
		 List<String> modelCodesList = new ArrayList<String>();
		 if(modelCodes != null && modelCodes.size() > 0){
			 modelCodesList.addAll(modelCodes);
			/* if(dbObj != null){
				 BasicDBList list = (BasicDBList) dbObj.get("m");
				 list.add(modelCodesList);
			 }*/
			 BasicDBObject updateRec = new BasicDBObject("k", searchString).append("m", modelCodesList).append("d", date);
			 isKeyWordModelCollection.update(new BasicDBObject("k",
					 searchString), new BasicDBObject("$set", updateRec), true,
						false);
		 }
	 }
}
