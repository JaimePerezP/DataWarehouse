package bbddavanzadas.datawarehouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBBroker {

	public static Connection connect() {
		Connection con = null;
		String driver = "com.mysql.cj.jdbc.Driver";
		String database = "datawarehouse-youtube"; // Hay que crear la DB
		String hostname = "127.0.0.1";
		String port = "3306";
		String url = "jdbc:mysql://" + hostname + ":" + port + "/" + database + "?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
		String username = "root";
		String password = "mysql";

		try {
			Class.forName(driver);
			con = DriverManager.getConnection(url, username, password);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}

		return con;
	}

}
