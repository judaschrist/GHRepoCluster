package cn.pku.sei.GHRC.graphdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.math3.linear.ArrayRealVector;

public class MySQLFetcher {
	private Connection conn = null;
	private Statement stmt = null;
	private Statement stmt1 = null;
	private Statement stmt2 = null;
	private static final String MEMBER_BY_SAME_SQL = "SELECT count(user_id) FROM project_members WHERE repo_id = %id1% AND user_id IN (SELECT user_id FROM project_members WHERE repo_id = %id2%)";
	private static final String PR_BY_SAME_SQL = "SELECT count(DISTINCT user_id) FROM pull_requests WHERE base_repo_id = %id1% AND user_id in (SELECT user_id FROM pull_requests WHERE base_repo_id = %id2%)";
	private static final String WATCHED_BY_SAME_SQL = "SELECT count(*) FROM watchers WHERE repo_id = %id1% and user_id IN (SELECT user_id from watchers WHERE repo_id = %id2%)";
	private static final String FORKED_BY_SAME_SQL = "SELECT count(*) FROM projects WHERE forked_from = %id1% and owner_id IN (SELECT owner_id from projects WHERE forked_from = %id2%)";
	private static final String COMMENTED_IN_ISSUE_BY_SAME_SQL = "SELECT count(DISTINCT a.user_id) FROM " +
			"(SELECT DISTINCT user_id FROM issue_comments where issue_id in (	" +
			"SELECT issue_id FROM issues WHERE repo_id = %id1%)) a, " +
			"(SELECT DISTINCT user_id FROM issue_comments where issue_id in (	" +
			"SELECT issue_id FROM issues WHERE repo_id = %id2%)) b " +
			"WHERE a.user_id = b.user_id";


	public MySQLFetcher() {
		Properties connectionProps = new Properties();
		connectionProps.put("user", "root");
		connectionProps.put("password", "zy123kq");
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/msr14", connectionProps);
			stmt = conn.createStatement();
			stmt1 = conn.createStatement();
			stmt2 = conn.createStatement();
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

		ArrayRealVector s = fetcher.getWatchVector(1);
		ArrayRealVector s2 = fetcher.getWatchVector(2);
		System.out.println(s.cosine(s2));
		fetcher.close();
	}
	
	public ArrayRealVector getWatchVector(long repoId) {
		try {
			ResultSet counts = stmt.executeQuery("SELECT count(id) from users");
			counts.next();
//			System.out.println("123");
			double[] ints = new double[counts.getInt(1)];
			ResultSet users = stmt1.executeQuery("SELECT id from users order by id");
			ResultSet watchers = stmt2.executeQuery("SELECT user_id from watchers WHERE repo_id = " + repoId + " ORDER BY user_id");
			if(watchers.next()){
				int i = 0;
				while (users.next()) {
					long id = users.getLong(1);
					if (watchers.getLong(1) == id) {
						ints[i] = 1;
						if (!watchers.next()) {
							break;
						}
					} 
					i++;
				}
			}			
			return new ArrayRealVector(ints);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public int countWatchedBySameNum(long ghid1, long ghid2) {
		try {
			ResultSet rs = stmt.executeQuery(WATCHED_BY_SAME_SQL.replace("%id1%", ghid1 + "").replace("%id2%", ghid2 + ""));
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			return 0;
		}
	}
	
	public int countForkedBySameNum(long ghid1, long ghid2) {
		try {
			ResultSet rs = stmt.executeQuery(FORKED_BY_SAME_SQL.replace("%id1%", ghid1 + "").replace("%id2%", ghid2 + ""));
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			return 0;
		}
	}
	
	public int countIssueCommentedBySameNum(long ghid1, long ghid2) {
		try {
			ResultSet rs = stmt.executeQuery(COMMENTED_IN_ISSUE_BY_SAME_SQL.replace("%id1%", ghid1 + "").replace("%id2%", ghid2 + ""));
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			return 0;
		}
	}
	
	public int countPrBySameNum(long ghid1, long ghid2) {
		try {
			ResultSet rs = stmt.executeQuery(PR_BY_SAME_SQL.replace("%id1%", ghid1 + "").replace("%id2%", ghid2 + ""));
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			return 0;
		}
	}
	
	public int countMemberBySameNum(long ghid1, long ghid2) {
		try {
			ResultSet rs = stmt.executeQuery(MEMBER_BY_SAME_SQL.replace("%id1%", ghid1 + "").replace("%id2%", ghid2 + ""));
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			return 0;
		}
	}
}
