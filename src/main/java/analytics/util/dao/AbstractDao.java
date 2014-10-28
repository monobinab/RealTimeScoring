package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;

import com.mongodb.DB;

public abstract class AbstractDao {
	static final Logger LOGGER = LoggerFactory
			.getLogger(AbstractDao.class);

	protected 	DB db;
	public AbstractDao() {
		this("default");
	}
	public AbstractDao(String server){
			try {
				db = DBConnection.getDBConnection(server);
			} catch (Exception e) {
				LOGGER.error("Unable to get DB connection", e);
			}
		}
}
