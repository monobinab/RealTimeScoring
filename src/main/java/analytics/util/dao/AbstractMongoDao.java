package analytics.util.dao;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.DB;

public abstract class AbstractMongoDao {
	
	static final Logger LOGGER = LoggerFactory.getLogger(AbstractDao.class);

	protected DB db;

	public AbstractMongoDao() {
		this("default");
	}

	public AbstractMongoDao(String server) {
		try {
			LOGGER.info("~~~~~~~~~~~~~~~ABSTRACT DAO~~~~~~~: " + System.getProperty(MongoNameConstants.IS_PROD));
			MongoDBConnectionWrapper mongoDBConnectionWrapper = MongoDBConnectionWrapper.getInstance();
			if(mongoDBConnectionWrapper != null){
				if(StringUtils.isNotEmpty(server) && server.equalsIgnoreCase("default")){
					db = mongoDBConnectionWrapper.db1;
					if(null == db){
						db = DBConnection.getDBConnection(server);
					}
				}if(StringUtils.isNotEmpty(server) && server.equalsIgnoreCase("server2") || server.equalsIgnoreCase("server2_2")){
					db = mongoDBConnectionWrapper.db2;
					if(null == db){
						db = DBConnection.getDBConnection(server);
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error("Unable to get DB connection", e);
		}
	}
}