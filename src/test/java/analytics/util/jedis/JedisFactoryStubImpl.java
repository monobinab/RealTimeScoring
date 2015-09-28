package analytics.util.jedis;

import com.fiftyonred.mock_jedis.MockJedis;

import redis.clients.jedis.Jedis;

public class JedisFactoryStubImpl implements JedisFactory{
	
	Jedis jedis;
	

	public Jedis getJedis() {
		return jedis;
	}


	public void setJedis(Jedis jedis) {
		this.jedis = jedis;
	}


	public Jedis createJedis(String host, int port){
		 if(getJedis() == null){
			 jedis = new MockJedis("test");
			 setJedis(jedis);
		 }
		 return jedis;
	 }
	 
}
