package analytics.util.jedis;

import com.fiftyonred.mock_jedis.MockJedis;

import redis.clients.jedis.Jedis;

public class JedisFactoryStubImpl implements JedisFactory{
	
	String host;
	int port;
		
	 public Jedis createJedis(String host, int port){
		 Jedis	jedis = new MockJedis("test");
		 return jedis;
	 }
	 
}
