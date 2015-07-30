package analytics.util.dao;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcernException;


public class XMLResponsysDAO extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(XMLResponsysDAO.class);
	DBCollection xmlResponsysCollection;


	public XMLResponsysDAO() {
		super();
		xmlResponsysCollection = db.getCollection("xmlResponsys");
		LOGGER.info("collection in XMLResponsysDAO: " + xmlResponsysCollection.getFullName());
	}
	
	
	public void addXMLResponsys(String l_id,  String xmlWithoutBOM,  
			String successFlag, String topologyName) {
		try {
			Date dNow = new Date( );
			DBObject respObj = new BasicDBObject();
			respObj.put(MongoNameConstants.L_ID, l_id);		
			respObj.put("xmlrequest", xmlWithoutBOM);
			respObj.put("successFlag", successFlag);
			respObj.put("retry_dt", "");
			respObj.put("topology", topologyName);
			SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss.SSS");
			respObj.put(MongoNameConstants.TIMESTAMP, ft.format(dNow));
			xmlResponsysCollection.insert(respObj);
			
			LOGGER.info("PERSIST: " + ft.format(dNow) + ", Topology: "+topologyName+", lid: " + l_id + ", "
					+", successFlag: "+successFlag);
		} catch (WriteConcernException e) {
			// TODO Auto-generated catch block
			LOGGER.error("Write concern Exception lid:"+ l_id +e.getMessage());
			e.printStackTrace();
		}
	
	}
	
	
	
	
	
	
	
	
}
