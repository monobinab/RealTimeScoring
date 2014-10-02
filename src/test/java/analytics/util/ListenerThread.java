package analytics.util;

import java.util.concurrent.LinkedBlockingQueue;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

public class ListenerThread extends Thread {
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

