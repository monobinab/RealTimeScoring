package analytics.util;

import com.fiftyonred.mock_jedis.MockJedis;

import redis.clients.jedis.Jedis;

public class StubJedisFactory {

	Jedis jedis = new MockJedis("test");
	
	public Jedis createJedis(){
		return jedis;
	}
}
