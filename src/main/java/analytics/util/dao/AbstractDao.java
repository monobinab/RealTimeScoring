package analytics.util.dao;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.DB;

public abstract class AbstractDao {
	
	static final Logger LOGGER = LoggerFactory.getLogger(AbstractDao.class);

	protected DB db;

	public AbstractDao() {
		this("dynamic");
	}

	/**
	public AbstractDao(String server) {
		try {
			LOGGER.info("~~~~~~~~~~~~~~~ABSTRACT DAO~~~~~~~: " + System.getProperty(MongoNameConstants.IS_PROD));
			if(StringUtils.isNotEmpty(server)){
				if(db == null){
						db = DBConnection.getDBConnection(server);
					}
				}
		} catch (Exception e) {
			LOGGER.error("Unable to get DB connection", e);
		}
	}*/
	
	public AbstractDao(String server) {
		try {
			LOGGER.info("~~~~~~~~~~~~~~~ABSTRACT DAO~~~~~~~: " + System.getProperty(MongoNameConstants.IS_PROD));
			//MongoDBConnectionWrapper class is used to take mongodb connections from API, 
			//please do not modify this code without consulting
			MongoDBConnectionWrapper mongoDBConnectionWrapper = MongoDBConnectionWrapper.getInstance();
			if(mongoDBConnectionWrapper != null){
				if(StringUtils.isNotEmpty(server) && server.equalsIgnoreCase("dynamic")){ 
					db = mongoDBConnectionWrapper.db1;
					if(null == db){
						db = DBConnection.getDBConnection(server);
					}
				}if(StringUtils.isNotEmpty(server) && server.equalsIgnoreCase("static")){
					db = mongoDBConnectionWrapper.db2;
					if(null == db){
						db = DBConnection.getDBConnection(server);
					}
				}if(StringUtils.isNotEmpty(server) && server.equalsIgnoreCase("mongo2")){
					db = mongoDBConnectionWrapper.db3;
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