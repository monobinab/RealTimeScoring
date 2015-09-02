package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MySQLDataSource;
import analytics.util.MySQLNameConstants;

import java.sql.Connection;

public abstract class AbstractMySQLDao {
	static final Logger LOGGER = LoggerFactory.getLogger(AbstractMySQLDao.class);

	protected Connection connection;

	public AbstractMySQLDao() {
		this("default");
	}

	public AbstractMySQLDao(String server) {
		try {
			LOGGER.info("~~~~~~~~~~~~~~~ABSTRACT MYSQL DAO~~~~~~~: " + System.getProperty(MySQLNameConstants.IS_PROD));
			connection = MySQLDataSource.getInstance().getConnection();
			if(connection == null) {
				LOGGER.error("MySQL DB connection is null");
			}
		} catch (Exception e) {
			LOGGER.error("Unable to get DB connection", e);
		}
	}
	
}
