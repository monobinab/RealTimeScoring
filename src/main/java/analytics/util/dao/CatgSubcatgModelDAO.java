package analytics.util.dao;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.mongodb.BasicDBList;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import analytics.util.objects.Sweep;

public class CatgSubcatgModelDAO extends AbstractDao{
		
    private DBCollection catSubCatCollection;
    
    public CatgSubcatgModelDAO() {
    	super();
    	catSubCatCollection = db.getCollection("catgSubcatgModel");
    }
    
	public List<Sweep> getCatSubCat(){
		List<Sweep> sweeps = new ArrayList<Sweep>();
		DBCursor catSubCatCursor = catSubCatCollection.find();
		for(DBObject catSubCatEntry: catSubCatCursor){
			Sweep sweep = new Sweep();
			if(catSubCatEntry != null){
				sweep.setCategory(catSubCatEntry.get("catg").toString());
				sweep.setSubCategory(catSubCatEntry.get("subcatg").toString());
				BasicDBList modelIdDBList = (BasicDBList)catSubCatEntry.get("modelId");
				List<String> modelIds = new ArrayList<String>();
				if(modelIdDBList != null && modelIdDBList.size() > 0){
					for (String key : modelIdDBList.keySet()) {
						if(StringUtils.isNotEmpty(key)){
							modelIds.add(String.valueOf(modelIdDBList.get(key)));
						}
					}
				}
				if(modelIds != null && modelIds.size() > 0){
					sweep.setModelId(modelIds);
				}
			}
			sweeps.add(sweep);
		}
		return sweeps;
	}
}
