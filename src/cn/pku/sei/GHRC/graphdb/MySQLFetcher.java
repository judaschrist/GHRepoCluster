package cn.pku.sei.GHRC.graphdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

public class MySQLFetcher {
	private Connection conn = null;
	private Statement stmt = null;
	private static final String WATCHED_BY_SAME_SQL = "SELECT count(*) FROM watchers WHERE repo_id = %id1% and user_id IN (SELECT user_id from watchers WHERE repo_id = %id2%)";
	private static final String FORKED_BY_SAME_SQL = "SELECT count(*) FROM projects WHERE forked_from = %id1% and owner_id IN (SELECT owner_id from projects WHERE forked_from = %id2%)";
	
	public MySQLFetcher() {
		Properties connectionProps = new Properties();
		connectionProps.put("user", "root");
		connectionProps.put("password", "woxnsk");
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(
					"jdbc:mysql://192.168.4.182:3306/msr14", connectionProps);
			stmt = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}
	
	public ResultSet getColumn(String formName, String columnName, String condition) {
		try {
			return stmt.executeQuery("SELECT "
					+ columnName
					+ " FROM "
					+ formName
					+ " WHERE "
					+ condition);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public void close() {
		try {
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws SQLException {
		MySQLFetcher fetcher = new MySQLFetcher();
		long ghid1 = 1;
		long ghid2 = 9;
		int s = fetcher.countForkedBySameNum(ghid1, ghid2);
		System.out.println(s);
		fetcher.close();

	}

	public int countWatchedBySameNum(long ghid1, long ghid2) {
		try {
			ResultSet rs = stmt.executeQuery(WATCHED_BY_SAME_SQL.replace("%id1%", ghid1 + "").replace("%id2%", ghid2 + ""));
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return 0;
		}
	}
	
	public int countForkedBySameNum(long ghid1, long ghid2) {
		try {
			ResultSet rs = stmt.executeQuery(FORKED_BY_SAME_SQL.replace("%id1%", ghid1 + "").replace("%id2%", ghid2 + ""));
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return 0;
		}
	}
	
	
	
}
