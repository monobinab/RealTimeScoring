package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;

import com.mongodb.DB;

public abstract class AbstractDao {
	static final Logger LOGGER = LoggerFactory
			.getLogger(ChangedMemberScoresDao.class);

	protected 	DB db;
	public AbstractDao() {
		{
			try {
				db = DBConnection.getDBConnection();
			} catch (Exception e) {
				LOGGER.error("Unable to get DB connection", e);
			}
		}
	}
}
