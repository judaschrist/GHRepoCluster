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
		ResultSet rs = fetcher.getColumn("projects", "id, url, name, description", "forked_from IS null");
		while (rs.next()) {
			System.out.println(rs.getInt(1) + " " + rs.getString(2) + " " + rs.getString(3) + " " + rs.getString(4));
		}
		System.out.println(rs.toString());
		fetcher.close();

	}
	
}
