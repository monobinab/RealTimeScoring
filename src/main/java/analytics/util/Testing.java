package analytics.util;

import java.util.ArrayList;

import redis.clients.jedis.Jedis;

public class Testing {

	public static void main(String[] args) {
		Jedis jedis = new Jedis("10.2.8.149", 11211, 800);
		/*ArrayList<String> list = new ArrayList<String>();
		list.add("010000000000P");*/
		jedis.rpush("signalBrowseFeed:BfdA+wNN9wbHYbJwCDAgLR+pf0s=", "00948021000P");
		jedis.rpush("signalBrowseFeed:BfdA+wNN9wbHYbJwCDAgLR+pf0s=", "00347580000P");//07133485000P
		jedis.rpush("signalBrowseFeed:BfdA+wNN9wbHYbJwCDAgLR+pf0s=", "07133485000P");
	}

}