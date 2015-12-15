package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import analytics.util.MongoNameConstants;
import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import analytics.util.objects.TagVariable;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class TagVariableDao extends AbstractDao {
	
	private DBCollection tagVariablesCollection;
	private Cache cache = null;

	public TagVariableDao() {
		super();
		tagVariablesCollection = db.getCollection("tagVariable");
		cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_TAGVARIABLECACHE);
    	if(null == cache){
			cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_TAGVARIABLECACHE);
	    	CacheBuilder.getInstance().setCaches(cache);
    	}
	}
	
	public Map<String, TagVariable> getTagVariables(){
		Map<String, TagVariable> tagVariablesMap = new HashMap<String, TagVariable>();
		List<TagVariable> tagVariables = this.getDBTagVariables();
		if(tagVariables != null && tagVariables.size() > 0){
			for(TagVariable tagVariable : tagVariables){
				tagVariablesMap.put(tagVariable.getTag(), tagVariable);
			}
		}
		return tagVariablesMap;
	}

	public Map<String, String> getTagVariable(String tag) {
		Map<String, String> tagVarMap = new HashMap<String, String>();
		if(StringUtils.isNotEmpty(tag)){
			String subTag = tag.substring(0,5);
			List<TagVariable> tagVariables = this.getDBTagVariables();
			if(tagVariables != null && tagVariables.size() > 0){
				for(TagVariable tagVariable : tagVariables){
					if(tagVariable.getTag().equalsIgnoreCase(subTag)){
						tagVarMap.put(tagVariable.getVariable(), tagVariable.getModelId());
					}
				}
			}
		}
		return tagVarMap;
	}

	public List<String> getTagVariablesList(List<String> tagsList) {
		List<String> tagVariablesList = new ArrayList<String>();
		if(tagsList != null && tagsList.size() > 0){
			for (String tag : tagsList) {
				if(StringUtils.isNotEmpty(tag)){
					List<TagVariable> tagVariables = this.getDBTagVariables();
					if(tagVariables != null && tagVariables.size() > 0){
						for(TagVariable tagVariable : tagVariables){
							if(tagVariable.getTag().equalsIgnoreCase(tag)){
								tagVariablesList.add(tagVariable.getVariable());
							}
						}
					}
				}
			}
		}
		return tagVariablesList;
	}

	public Map<Integer, String> getTagModelIds(Set<String> tagsList) {
		Map<Integer, String> tagModelMap = new HashMap<Integer, String>();
		if(tagsList != null && tagsList.size() > 0){
			for (String tag : tagsList) {
				if(StringUtils.isNotEmpty(tag)){
					List<TagVariable> tagVariables = this.getDBTagVariables();
					if(tagVariables != null && tagVariables.size() > 0){
						for(TagVariable tagVariable : tagVariables){
							if(tagVariable.getTag().equalsIgnoreCase(tag)){
								tagModelMap.put(Integer.parseInt(tagVariable.getModelId()), tagVariable.getTag());
							}
						}
					}
				}
			}
		}
		return tagModelMap;
	}
	
	public Set<Integer> getModels(){
		Set<Integer> models = new HashSet<Integer>();
		List<TagVariable> tagVariables = this.getDBTagVariables();
		if(tagVariables != null && tagVariables.size() > 0){
			for(TagVariable tagVariable : tagVariables){
				models.add(Integer.parseInt(tagVariable.getModelId()));
			}
		}
		return models;
	}
	
	public Map<Integer,String> getModelTags(){
		Map<Integer,String> modelTags = new HashMap<Integer,String>();
		List<TagVariable> tagVariables = this.getDBTagVariables();
		if(tagVariables != null && tagVariables.size() > 0){
			for(TagVariable tagVariable : tagVariables){
				modelTags.put(Integer.parseInt(tagVariable.getModelId()), tagVariable.getTag());
			}
		}
		return modelTags;
	}
	
	public Integer getmodelIdFromTag(String tag) {
		if(StringUtils.isNotEmpty(tag)){
			String subTag = tag.substring(0,5);
			List<TagVariable> tagVariables = this.getDBTagVariables();
			if(tagVariables != null && tagVariables.size() > 0){
				for(TagVariable tagVariable : tagVariables){
					if(tagVariable.getTag().equalsIgnoreCase(subTag)){
						return Integer.parseInt(tagVariable.getModelId());
					}
				}
			}
		}
		return null;
	}
	
	
	@SuppressWarnings("unchecked")
	private List<TagVariable> getDBTagVariables(){
		String cacheKey = CacheConstant.RTS_TAGVARIABLE_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (List<TagVariable>) element.getObjectValue();
		}else{
		List<TagVariable> tagVariables = new ArrayList<TagVariable>();
		DBCursor cursor = tagVariablesCollection.find();
		if(cursor != null && cursor.count() > 0){
			while(cursor.hasNext()){
				DBObject modelObj = cursor.next();
				if(modelObj != null){
					TagVariable tagVariable = new TagVariable();
					tagVariable.setModelId(String.valueOf(modelObj.get(MongoNameConstants.TAG_VAR_MODEL)));
					tagVariable.setTag((String) modelObj.get(MongoNameConstants.TAG_VAR_MDTAG));
					tagVariable.setVariable((String) modelObj.get(MongoNameConstants.TAG_VAR_VAR));
					tagVariables.add(tagVariable);
				}
			}
		}
		if(tagVariables != null && tagVariables.size() > 0){
			cache.put(new Element(cacheKey, (List<TagVariable>) tagVariables));
		}
		return tagVariables;
		}
	}
	
	public Map<Integer, String> getTagFromModelId(){
		Map<Integer, String> tagVariablesMap = new HashMap<Integer, String>();
		DBCursor tagVariables = tagVariablesCollection.find();
		while(tagVariables.hasNext()){
			DBObject tagVariableObject = tagVariables.next();
			if(tagVariableObject!=null)
			{
				tagVariablesMap.put(Integer.valueOf(tagVariableObject.get(MongoNameConstants.TAG_VAR_MODEL).toString()), 
						tagVariableObject.get(MongoNameConstants.TAG_VAR_MDTAG).toString());
			}
		}
		return tagVariablesMap;
	}
}
