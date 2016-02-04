package analytics.util;

import java.net.UnknownHostException;
import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class MongoConnectionHelper {

	private static MongoClient mongoSingletonProd1 = null;
	private static MongoClient mongoSingletonProd2 = null;
	private static MongoClient mongoSingletonProd2_2 = null;

    public static synchronized MongoClient getMongoClientProd1(List<ServerAddress> replicaSetServers) throws UnknownHostException {
        if (mongoSingletonProd1 == null) {
            synchronized (MongoConnectionHelper.class) {
                if (mongoSingletonProd1 == null) {
                	mongoSingletonProd1 = new MongoClient(replicaSetServers);
                }
            }
        }
        return mongoSingletonProd1;
    }
    
    public static synchronized MongoClient getMongoClientProd2(List<ServerAddress> replicaSetServers) throws UnknownHostException {
        if (mongoSingletonProd2 == null) {
            synchronized (MongoConnectionHelper.class) {
                if (mongoSingletonProd2 == null) {
                   	mongoSingletonProd2 = new MongoClient(replicaSetServers);
                }
            }
        }
        return mongoSingletonProd2;
    }
    public static synchronized MongoClient getMongoClientProd2_2(List<ServerAddress> replicaSetServers) throws UnknownHostException {
        if (mongoSingletonProd2_2 == null) {
            synchronized (MongoConnectionHelper.class) {
                if (mongoSingletonProd2_2 == null) {
                	mongoSingletonProd2_2 = new MongoClient(replicaSetServers);
                }
            }
        }
        return mongoSingletonProd2_2;
    }
}
