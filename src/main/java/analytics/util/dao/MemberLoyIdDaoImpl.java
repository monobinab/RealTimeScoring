package analytics.util.dao;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.configuration.ConfigurationException;

import analytics.util.MySQLDataSource;



public class MemberLoyIdDaoImpl implements MemberLoyIdDao{

	@Override
	public String getLoyaltyId(String l_id) {
		Connection connection = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		 String loyId = null;
		try {
            connection = MySQLDataSource.getInstance().getConnection();//do null check 
            statement = connection.prepareStatement("select loy_id from l_id_xref where l_id=?");
            statement.setString(1, l_id);
            ResultSet rs = statement.executeQuery();
             while (rs.next()) {
                  System.out.println("loyalty id: " + rs.getString("loy_id"));
                  loyId = rs.getString("loy_id");
             }
        } catch (SQLException e) {
			e.printStackTrace();
		} catch (ConfigurationException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		} finally {
			if (resultSet != null)
				try {
					resultSet.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			if (statement != null)
				try {
					statement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			if (connection != null)
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}
			return loyId;
	}

}