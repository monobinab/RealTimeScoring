package analytics.spout;


import analytics.util.RedisConnection;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static backtype.storm.utils.Utils.tuple;

public class RedisPubSubSpout extends BaseRichSpout {

    private static Logger LOGGER = LoggerFactory.getLogger(RedisPubSubSpout.class);

    SpoutOutputCollector _collector;
     final String pattern;
    LinkedBlockingQueue<String> queue;
    JedisPool pool;
    final int number;
    String environment;

    public RedisPubSubSpout( int number, String pattern, String systemProperty) {
        this.number = number;
        this.pattern = pattern;
        environment = systemProperty;
    }
    class ListenerThread extends Thread {
        LinkedBlockingQueue<String> queue;
        JedisPool pool;
        String pattern;

        public ListenerThread(LinkedBlockingQueue<String> queue, JedisPool pool, String pattern) {
            this.queue = queue;
            this.pool = pool;
            this.pattern = pattern;
        }

        public void run() {

            JedisPubSub listener = new JedisPubSub() {

                @Override
                public void onMessage(String channel, String message) {
                    queue.offer(message);
                }

                @Override
                public void onPMessage(String pattern, String channel, String message) {
                    queue.offer(message);
                }

                @Override
                public void onPSubscribe(String channel, int subscribedChannels) {
                    // TODO Auto-generated method stub

                }

                @Override
                public void onPUnsubscribe(String channel, int subscribedChannels) {
                    // TODO Auto-generated method stub

                }

                @Override
                public void onSubscribe(String channel, int subscribedChannels) {
                    // TODO Auto-generated method stub

                }

                @Override
                public void onUnsubscribe(String channel, int subscribedChannels) {
                    // TODO Auto-generated method stub

                }
            };

            Jedis jedis = pool.getResource();
            try {
                jedis.psubscribe(listener, pattern);
            } finally {
                pool.returnResource(jedis);
            }
        }
    };

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        queue = new LinkedBlockingQueue<String>(1000);
     //   HostPortUtility.getInstance(conf.get("nimbus.host").toString());
        String[] redisServers = RedisConnection.getServers(environment);
        pool = new JedisPool(new JedisPoolConfig(), redisServers[number], 6379);
        System.out.println(redisServers[number]);
        ListenerThread listener = new ListenerThread(queue, pool, pattern);
        listener.start();
    }

    public void close() {
        pool.destroy();
    }

    public void nextTuple() {
        String ret = queue.poll();
        if(ret==null) {
            Utils.sleep(50);
        } else {
            emit(ret);
        }
    }

    protected void emit(String ret) {
        _collector.emit(tuple(ret));
    }

    public void ack(Object msgId) {
        // TODO Auto-generated method stub

    }

    public void fail(Object msgId) {
        // TODO Auto-generated method stub

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }

    public boolean isDistributed() {
        return false;
    }
}
