package analytics.spout;

import com.mongodb.DBObject;

import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.*;

import java.io.Serializable;
import java.util.List;

public class MongoOpLogSpout extends MongoSpoutBase implements Serializable {

  private static final long serialVersionUID = 5498284114575395939L;
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoOpLogSpout.class);

  private static String[] collectionNames = {"oplog.$main", "oplog.rs"};
  private String filterByNamespace;

  public MongoOpLogSpout(String url) {
    super(url, "local", collectionNames, null, null);
  }

  public MongoOpLogSpout(String url, DBObject query) {
    super(url, "local", collectionNames, query, null);
  }

  public MongoOpLogSpout(String url, String filterByNamespace) {
    super(url, "local", collectionNames, null, null);
    this.filterByNamespace = filterByNamespace;
  }

  public MongoOpLogSpout(String url, DBObject query, String filterByNamespace) {
    super(url, "local", collectionNames, query, null);
    this.filterByNamespace = filterByNamespace;
  }

  public MongoOpLogSpout(String url, MongoObjectGrabber mapper) {
    super(url, "local", collectionNames, null, mapper);
  }

  public MongoOpLogSpout(String url, DBObject query, MongoObjectGrabber mapper) {
    super(url, "local", collectionNames, query, mapper);
  }

  public MongoOpLogSpout(String url, String filterByNamespace, MongoObjectGrabber mapper) {
    super(url, "local", collectionNames, null, mapper);
    this.filterByNamespace = filterByNamespace;
  }

  public MongoOpLogSpout(String url, DBObject query, String filterByNamespace, MongoObjectGrabber mapper) {
    super(url, "local", collectionNames, query, mapper);
    this.filterByNamespace = filterByNamespace;
  }

  @Override
  protected void processNextTuple() {
    DBObject object = this.queue.poll();
    // If we have an object, let's process it, map and emit it
    if (object != null) {
      String operation = object.get("op").toString();
      // Check if it's a i/d/u operation and push the data
      if ("i".equals(operation) || "d".equals(operation) || "u".equals(operation)) {
        if (LOGGER.isInfoEnabled()) LOGGER.info(object.toString());

        // Verify if it's the correct namespace
        if (this.filterByNamespace != null && !this.filterByNamespace.equals(object.get("ns").toString())) {
          return;
        }

        // Map the object to a tuple
        List<Object> tuples = this.mapper.map(object);

        // Contains the objectID
        String objectId = null;
        // Extract the ObjectID
        if ("i".equals(operation) || "d".equals(operation)) {
          if (object.get("o") != null && ((BSONObject) object.get("o")).get("_id") != null) {
            objectId = ((BSONObject) object.get("o")).get("_id").toString();
            // Emit the tuple collection
            this.collector.emit(tuples, objectId);
          }
        } else if ("u".equals(operation)) {
          if (object.get("o2") != null && ((BSONObject) object.get("o2")).get("_id") != null) {
            objectId = ((BSONObject) object.get("o2")).get("_id").toString();
            // Emit the tuple collection
            this.collector.emit(tuples, objectId);
          }
        }
      }
    }
  }
}
