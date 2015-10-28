package analytics.util.dao;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class HoldMembersDAO extends AbstractMySQLDao {

	public List<String> getHoldMembersList() {

		List<String> members = new ArrayList();
		ResultSet rs = null;
		PreparedStatement statement = null;

		StringBuffer query = new StringBuffer();
		query.append("SELECT loy_id FROM cp_restricted_members ");
		try {
			statement = connection.prepareStatement(query.toString());
			rs = statement.executeQuery();

			while (rs.next()) {
				String loyID = rs.getString("loy_id");
				members.add(loyID);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rs != null)
				try {
					rs.close();
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
		return members;

	}

}
