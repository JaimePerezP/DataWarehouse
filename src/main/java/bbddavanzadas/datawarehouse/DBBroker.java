package bbddavanzadas.datawarehouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class DBBroker {

	public static Connection connect() {
		Connection con = null;
		String driver = "com.mysql.cj.jdbc.Driver";
//		Database de Jaime
//		String database = "datawarehouse-youtube";
//		Database de Jorge
		String database = "datawarehouse"; // Hay que crear la DB
		String hostname = "127.0.0.1";
		String port = "3306";
		String url = "jdbc:mysql://" + hostname + ":" + port + "/" + database
				+ "?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
		String username = "root";
//		Contraseña de Jaime
//		String password = "mysql";
//		Contraseña de Jorge
		String password = "jorgejaime123";

		try {
			Class.forName(driver);
			con = DriverManager.getConnection(url, username, password);
			System.out.println("DB connected");
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}

		return con;
	}

	public static void createTablesMySQL(String sql) {
		System.out.println(sql);
		try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
			stmt.execute(sql);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	public static void createTables() {
		createTablesMySQL(
				"CREATE TABLE IF NOT EXISTS Trending_Video_Youtube (\n id_video varchar(255) PRIMARY KEY,\n id_dates integer NOT NULL,\n id_options integer NOT NULL,\n id_group integer NOT NULL,\n id_statistics integer NOT NULL,\n title varchar(255) NOT NULL,\n channel_title varchar(255) NOT NULL,\n description varchar(255) NOT NULL,\n thumbnail_link varchar(255) NOT NULL\n);\n");
		createTablesMySQL(
				"CREATE TABLE IF NOT EXISTS Video_Date (\n id_dates integer PRIMARY KEY,\n trending_date date NOT NULL,\n publish_time date NOT NULL\n);\n");
		createTablesMySQL(
				"CREATE TABLE IF NOT EXISTS Video_Options (\n id_options integer PRIMARY KEY,\n retings_disabled varchar(255) NOT NULL,\n comments_disabled varchar(255) NOT NULL,\n video_error_or_removed varchar(255) NOT NULL\n);\n");
		createTablesMySQL(
				"CREATE TABLE IF NOT EXISTS Video_Group (\n id_group integer PRIMARY KEY,\n category_id integer NOT NULL,\n tags varchar(255) NOT NULL\n);\n");
		createTablesMySQL(
				"CREATE TABLE IF NOT EXISTS Video_Statistics (\n id_statistics integer PRIMARY KEY,\n views integer NOT NULL,\n likes integer NOT NULL,\n dislike integer NOT NULL,\n comments_count varchar(255) NOT NULL \n);\n");
	}

	public static void insert() throws SQLException {
		String query = "insert into Video_Group (id_group, category_id, tags)" + " values (?, ?, ?)";
		
		Connection con = connect();
		// create the mysql insert preparedstatement
		PreparedStatement preparedStmt = con.prepareStatement(query);
		preparedStmt.setInt(1, 1);
		preparedStmt.setInt(2, 2);
		preparedStmt.setString(3, "Bien");

		// execute the preparedstatement
		preparedStmt.execute();

	}

}
