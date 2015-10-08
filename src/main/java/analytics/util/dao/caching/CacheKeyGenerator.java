package analytics.util.dao.caching;

import java.util.UUID;

public class CacheKeyGenerator {
	
	public static String generateCacheKey(){
		return UUID.randomUUID().toString();
	}
	
	public static void main(String[] args) {
		String key = generateCacheKey();
		System.out.println(key);
	}
}
