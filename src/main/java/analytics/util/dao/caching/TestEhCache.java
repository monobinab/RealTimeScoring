package analytics.util.dao.caching;

import analytics.util.dao.caching.CacheBuilder;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

public class TestEhCache{
	
	public TestEhCache(){
		for(int i = 0; i< 10; i++){
			//this.testCache();
		}
	}
	/**
	public void testCache(){
		System.out.println("Called");
		Cache cache = CacheBuilder.getInstance().getCache(RTSCacheConstant.RTS_CACHE_MODELPERCENTILECACHE);
		if(cache != null){
			Element ele = CacheWrapper.getInstance().isCacheKeyExist(cache, "3");
			if(ele != null && ele.getObjectKey().equals("3")){
				String val = (String)ele.getObjectValue();
				System.out.println(val);
			}else{
				System.out.println("Value Not Exist");
				//3. Put few elements in cache
				cache.put(new Element("1","Jan"));
				cache.put(new Element("2",new Integer(50)));
				cache.put(new Element("3",new Long(200000)));
				cache.put(new Element("4",new String("Good Luck")));
				
				//4. Get element from cache
				//Element ele = cache.get("2");
				
				//5. Print out the element
				//String output = (ele == null ? null : ele.getObjectValue().toString());
				//System.out.println(output);
				
				//6. Is key in cache?
				//System.out.println(cache.isKeyInCache("3"));
				//System.out.println(cache.isKeyInCache("10"));
			}
		}
	}
	*/
	public static void main(String[] args) {
		new TestEhCache();
	}
}