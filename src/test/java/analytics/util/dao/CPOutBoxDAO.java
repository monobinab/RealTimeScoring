package analytics.util.dao;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import cpstest.CPOutBoxItem;
import analytics.util.dao.AbstractMySQLDao;



public class CPOutBoxDAO extends AbstractMySQLDao {
	

	public int getCPRecordCount() {
		int count = 0;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		Date dNow = new Date();
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
		String today = ft.format(dNow);
		try {
			statement = connection
					.prepareStatement("SELECT count(*)  FROM cp_outbox where send_date<=? and  sent_datetime is null and status=0 ");
			statement.setString(1, today);
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				System.out.println("count is " + rs.getInt(1));
				count = rs.getInt(1);
			}
		} catch (SQLException e) {
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
		return count;

	}

	public List<CPOutBoxItem> getCPOutBoxList(int startID, int endID) {

		// TODO Auto-generated method stub
		List<CPOutBoxItem> cpOutBoxList = null;
		;
		PreparedStatement statement = null;
		ResultSet rs = null;
		Date dNow = new Date();
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String today = ft.format(dNow);
		try {
			StringBuffer query = new StringBuffer();
			query.append("SELECT * FROM cp_outbox where send_date<=? and sent_datetime is null and status=0 and email_pkg_id>= ? ");
			if (startID != endID) {
				query.append(" and email_pkg_id< ? ");
			}
			query.append(" order by email_pkg_id asc LIMIT 100");
			statement = connection.prepareStatement(query.toString());
			statement.setString(1, today);
			statement.setInt(2, startID);
			if (startID != endID)
				statement.setInt(3, endID);
			// System.out.println(statement.);
			rs = statement.executeQuery();
			if (rs.isBeforeFirst()) {
				cpOutBoxList = new ArrayList();
			}
			while (rs.next()) {
				CPOutBoxItem cpOutBoxItem = new CPOutBoxItem();
				cpOutBoxItem.setAdded_datetime(sdf.format(rs.getTimestamp("added_datetime")));
				cpOutBoxItem.setBu(rs.getString("bu"));
				cpOutBoxItem.setCust_event_name(rs.getString("cust_event_name"));
				cpOutBoxItem.setCustomer_id(rs.getString("customer_id"));
				cpOutBoxItem.setEmail_pkg_id(rs.getInt("email_pkg_id"));
				cpOutBoxItem.setKmart_opt_in(rs.getString("kmart_opt_in"));
				cpOutBoxItem.setLoy_id(rs.getString("loy_id"));
				cpOutBoxItem.setMd_tag(rs.getString("md_tag"));
				cpOutBoxItem.setOccasion_name(rs.getString("occasion_name"));
				cpOutBoxItem.setSears_opt_in(rs.getString("sears_opt_in"));
				cpOutBoxItem.setSend_date(ft.format(rs.getDate("send_date")));
				cpOutBoxItem.setSent_datetime(rs.getTimestamp("sent_datetime"));
				cpOutBoxItem.setStatus(rs.getInt("status"));
				cpOutBoxItem.setSub_bu(rs.getString("sub_bu"));
				cpOutBoxItem.setSyw_opt_in(rs.getString("syw_opt_in"));
				cpOutBoxList.add(cpOutBoxItem);

			}
		} catch (SQLException e) {
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

		return cpOutBoxList;
	}

	public void updateSent(CPOutBoxItem cpOutBoxItem) {
		// TODO Auto-generated method stub
		int count = 0;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		Date dNow = new Date();
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
		String today = ft.format(dNow);
		try {

			statement = connection
					.prepareStatement("update cp_outbox set sent_datetime=? status=? where email_pkg_id=? and loy_id=?");
			statement.setTimestamp(1, new java.sql.Timestamp(cpOutBoxItem.getSent_datetime().getTime()));
			statement.setInt(2, cpOutBoxItem.getStatus());
			statement.setInt(3, cpOutBoxItem.getEmail_pkg_id());
			statement.setString(4, cpOutBoxItem.getLoy_id());
			statement.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
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

	}

	public int getNthRowID(int i) {
		// TODO Auto-generated method stub
		int emailPkgId = 0;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		Date dNow = new Date();
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
		String today = ft.format(dNow);
		int limit = 1;
		int offset = i - 1;
		try {

			statement = connection
					.prepareStatement("SELECT email_pkg_id FROM cp_outbox where send_date<=? and  sent_datetime is null and status=0 ORDER BY email_pkg_id ASC  LIMIT "
							+ limit + " OFFSET " + offset);
			statement.setString(1, today);
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {

				emailPkgId = rs.getInt("email_pkg_id");
				System.out.println("The email PKG Id is " + emailPkgId);
			}
		} catch (SQLException e) {
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
		return emailPkgId;

	}

	public void insertRow(CPOutBoxItem cpOutBoxItem) {

		PreparedStatement statement = null;
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		try {

			statement = connection
					.prepareStatement("INSERT INTO rts_member.cp_outbox (loy_id,md_tag,send_date,status,added_datetime,sent_datetime,bu,sub_bu,occasion_name) VALUES (?,?,?,?,?,?,?,?,?)");
			
			statement.setString(1, cpOutBoxItem.getLoy_id());
			statement.setString(2, cpOutBoxItem.getMd_tag());
			statement.setDate(3,  new java.sql.Date(ft.parse(cpOutBoxItem.getSend_date()).getTime()));
			statement.setInt(4, cpOutBoxItem.getStatus());
			statement.setTimestamp(5, new java.sql.Timestamp(sdf.parse(cpOutBoxItem.getAdded_datetime()).getTime()));	
			if(cpOutBoxItem.getStatus()==1)
			statement.setTimestamp(6, new java.sql.Timestamp((cpOutBoxItem.getSent_datetime()).getTime()));	
			else
				statement.setTimestamp(6,null);
			statement.setString(7, cpOutBoxItem.getBu());
			statement.setString(8, cpOutBoxItem.getSub_bu());
			statement.setString(9, cpOutBoxItem.getOccasion_name());
			statement.executeUpdate();

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
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

	
		// TODO Auto-generated method stub
		
	}

	public CPOutBoxItem getQueuedItem(String loy_id, String md_tag, int status) {

		// TODO Auto-generated method stub
		List<CPOutBoxItem> cpOutBoxList = null;
		PreparedStatement statement = null;
		ResultSet rs = null;
		Date dNow = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
		String today = ft.format(dNow);
		CPOutBoxItem cpOutBoxItem = new CPOutBoxItem();
		try {
			StringBuffer query = new StringBuffer();
			query.append("SELECT * FROM cp_outbox where loy_id=? and md_tag=? and status=? ");
			statement = connection.prepareStatement(query.toString());
			statement.setString(1, loy_id);
			statement.setString(2, md_tag);
			statement.setInt(3, status);			
			rs = statement.executeQuery();
			if (rs.isBeforeFirst()) {
				cpOutBoxList = new ArrayList();
			}
			//while (rs.next()) {
			if(rs.next())
				{ 
				cpOutBoxItem.setAdded_datetime(sdf.format(rs.getDate("added_datetime")));
				cpOutBoxItem.setBu(rs.getString("bu"));
				cpOutBoxItem
						.setCust_event_name(rs.getString("cust_event_name"));
				cpOutBoxItem.setCustomer_id(rs.getString("customer_id"));
				cpOutBoxItem.setEmail_pkg_id(rs.getInt("email_pkg_id"));
				cpOutBoxItem.setKmart_opt_in(rs.getString("kmart_opt_in"));
				cpOutBoxItem.setLoy_id(rs.getString("loy_id"));
				cpOutBoxItem.setMd_tag(rs.getString("md_tag"));
				cpOutBoxItem.setOccasion_name(rs.getString("occasion_name"));
				cpOutBoxItem.setSears_opt_in(rs.getString("sears_opt_in"));
				cpOutBoxItem.setSend_date(ft.format(rs.getDate("send_date")));
				cpOutBoxItem.setSent_datetime(rs.getDate("sent_datetime"));
				cpOutBoxItem.setStatus(rs.getInt("status"));
				cpOutBoxItem.setSub_bu(rs.getString("sub_bu"));
				cpOutBoxItem.setSyw_opt_in(rs.getString("syw_opt_in"));
				//cpOutBoxList.add(cpOutBoxItem);

			}
		} catch (SQLException e) {
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

		return cpOutBoxItem;
	}
	
	public int getQueuedCount(String lyl_id_no) {
		PreparedStatement statement = null;
		ResultSet rs = null;
		int queueCount = 0;
		String query = "SELECT count(*) as queuedCount FROM cp_outbox where loy_id=? ;";
		try {
			statement = connection.prepareStatement(query);
			statement.setString(1, lyl_id_no);
			rs = statement.executeQuery();
			while(rs.next()){
				queueCount = rs.getInt("queuedCount");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			try {
				statement.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return queueCount;
		
	}

}

