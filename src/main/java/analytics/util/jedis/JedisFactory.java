package analytics.util.jedis;

import redis.clients.jedis.Jedis;

public interface JedisFactory {
	
	public Jedis createJedis(String host, int port);

}
