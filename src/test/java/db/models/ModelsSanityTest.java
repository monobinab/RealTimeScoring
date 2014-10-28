package db.models;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import analytics.util.dao.ModelsDao;

public class ModelsSanityTest {
	@Test
	public void checkModelsMatchDb() throws ClassNotFoundException, SQLException{
		Map<Integer,String> modelNamesMap = new ModelsDao().getModelNames();
	
		StringBuilder queryColumns = new StringBuilder();
		for(String modelName: modelNamesMap.values()){
			queryColumns.append(modelName);
			queryColumns.append(",");
		}
		String query = "SELECT "+queryColumns.substring(0, queryColumns.length()-1);
		query+=" FROM crm_perm_tbls.lliu4_allmodel_lyl_ranks_lstwk where lyl_id_no=7081257366894445";
        Class.forName("com.teradata.jdbc.TeraDriver");
        Connection connection = DriverManager.getConnection("jdbc:teradata://EDWPROD1COP2.intra.searshc.com", "crm_prod_batch", "onecustomer");
        Statement statement = connection.createStatement();
      
		ResultSet resultSet = statement.executeQuery(query);
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(75, resultSetMetaData.getColumnCount());
	}
}
