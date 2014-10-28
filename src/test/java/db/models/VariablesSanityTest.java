package db.models;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import analytics.util.dao.ModelsDao;
import analytics.util.dao.VariableDao;
import analytics.util.objects.Variable;

public class VariablesSanityTest {
	@Test
	public void checkModelsMatchDb() throws ClassNotFoundException, SQLException{
		System.setProperty("rtseprod", "true");
		List<Variable> variables = new VariableDao().getVariables();
		System.setProperty("rtseprod", "test");
		StringBuilder queryColumns = new StringBuilder();
		for(Variable variable: variables){
			Assert.assertNotNull((Double)variable.getCoefficient());
			int vid = Integer.parseInt(variable.getVid());
			Assert.assertTrue(vid>0);
			if(vid<1000 && !variable.getName().startsWith("MONTH")){
				queryColumns.append(variable.getName());
				queryColumns.append(",");
			}
			
		}
		String query = "SELECT "+queryColumns.substring(0, queryColumns.length()-1);
		query+=" From crm_perm_tbls.lliu4_SYWR_SRS_KMT_MSM_lstwk as a Join crm_perm_tbls.lliu4_allmodel_lyl_ranks_lstwk as c on a.cnsmr_id=c.cnsmr_id where c.lyl_id_no=7081257366894445;";
        Class.forName("com.teradata.jdbc.TeraDriver");
        Connection connection = DriverManager.getConnection("jdbc:teradata://EDWPROD1COP2.intra.searshc.com", "crm_prod_batch", "onecustomer");
        Statement statement = connection.createStatement();
      
		ResultSet resultSet = statement.executeQuery(query);
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Assert.assertEquals(687, resultSetMetaData.getColumnCount());
	}
}
