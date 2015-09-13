package analytics.util.jedis;

import redis.clients.jedis.Jedis;

public class JedisFactoryImpl implements JedisFactory{
	
	String host;
	int port;
			
	 public Jedis createJedis(String host, int port){
		 Jedis	jedis = new Jedis(host, port, 800);
		 return jedis;
	 }
	 
}
