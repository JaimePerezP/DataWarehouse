package bbddavanzadas.datawarehouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBBroker {

	public static Connection connect() {
		Connection con = null;
		
	    String driver = "com.mysql.jdbc.Driver";
	    String database = "databasemovies"; // Hay que crear la DB
	    String hostname = "localhost";
	    String port = "3306";
	    String url = "jdbc:mysql://" + hostname + ":" + port + "/" + database + "?&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
	    String username = "root";
	    String password = "jorgejaime123";
	    
	    try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

		
		return con;
	}

}
