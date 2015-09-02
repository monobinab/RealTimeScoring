package analytics.util;

import java.net.UnknownHostException;
import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;

public class MongoConnectionHelperProd2 {

	private static MongoClient mongoSingleton = null;

    public static synchronized MongoClient getMongoClient(List<ServerAddress> replicaSetServers, MongoClientOptions options) throws UnknownHostException {
        if (mongoSingleton == null) {
            synchronized (MongoConnectionHelperProd2.class) {
                if (mongoSingleton == null) {
                    mongoSingleton = new MongoClient(replicaSetServers, options);
                }
            }
        }
        return mongoSingleton;
    }
}
