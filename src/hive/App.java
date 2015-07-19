package hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class App {

	public static void main(String[] args) throws Exception {
		Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection("jdbc:hive://hadoop0:10000/default", "", "");
		Statement stmt = con.createStatement();
		String sql = "SELECT * FROM default.t1";
		ResultSet res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getInt(1));
		}
	}
}
