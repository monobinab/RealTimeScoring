package analytics.util;

import java.util.ArrayList;

import redis.clients.jedis.Jedis;

public class RedisInsert {

	public static void main(String[] args) {
		Jedis jedis = new Jedis ("10.2.8.175", 11211);
		jedis.connect();
		String loyId = "signalTestingFeed:" + "BfdA+wNN9wbHYbJwCDAgLR+pf0s=";
		if (!(jedis.exists(loyId))) {
			jedis.rpush(loyId, Long.toString(System.currentTimeMillis()));
		}
	
		jedis.rpush(loyId, "00949062000P");
		jedis.rpush(loyId, "09621914000P");
		jedis.rpush(loyId, "07149683000P");
		
		jedis.rpush(loyId, "00949062000P");
		jedis.rpush(loyId, "09621914000P");
		jedis.rpush(loyId, "07149683000P");
	}
}
