package bbddavanzadas.datawarehouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class DBBroker {

    private static Connection con;

    public DBBroker() throws ClassNotFoundException, SQLException {
        connect();
    }

    public static void connect() throws ClassNotFoundException, SQLException {
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

        Class.forName(driver);
        con = DriverManager.getConnection(url, username, password);
        System.out.println("DB connected");
    }

    public void closeConnection() throws SQLException {
        con.close();
    }

    public void exeSQLCode(String code) throws SQLException {
        con.createStatement().execute(code);
    }

    public void createTables() throws SQLException {
        exeSQLCode(
                "CREATE TABLE IF NOT EXISTS Hechos (\n id_video varchar(255) ,\n titulo_canal varchar(255),\n nombre_categoria varchar(255),\n id_tiempo integer,\n pais varchar(255),\n visitas integer NOT NULL,\n likes integer NOT NULL,\n dislikes integer NOT NULL,\n num_comentarios integer NOT NULL, \n PRIMARY KEY(id_video, titulo_canal, nombre_categoria, id_tiempo, pais)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8;\n");
        exeSQLCode(
                "CREATE TABLE IF NOT EXISTS Dimension_Video (\n id_video varchar(255) PRIMARY KEY,\n titulo varchar(255) NOT NULL,\n descripcion varchar(7022) NOT NULL,\n tags varchar(1500) NOT NULL,\n comments_disabled BOOLEAN NOT NULL,\n retings_disabled BOOLEAN NOT NULL,\n video_error_or_removed BOOLEAN NOT NULL,\n thumbnail_link varchar(255) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;\n");
        exeSQLCode(
                "CREATE TABLE IF NOT EXISTS Dimension_Tiempo (\n id_tiempo integer PRIMARY KEY,\n fecha_publi DATE NOT NULL,\n año_publi integer NOT NULL,\n mes_publi integer NOT NULL,\n dia_publi integer NOT NULL,\n hora_publi TIME NOT NULL,\n fecha_trend DATE NOT NULL,\n año_trend integer NOT NULL,\n mes_trend integer NOT NULL,\n dia_trend integer NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;\n");
        exeSQLCode("ALTER TABLE Hechos ADD FOREIGN KEY (id_video) REFERENCES Dimension_Video(id_video);");
        exeSQLCode("ALTER TABLE Hechos ADD FOREIGN KEY (id_tiempo) REFERENCES Dimension_Tiempo(id_tiempo);");
    }

    public void insertVideo(String[] video) throws SQLException {
        String id_video = video[0];
        String titulo_video = video[2];
        String tags = video[6];
        String thumbnail_link = video[11];
        int comments_disabled = Boolean.getBoolean(video[12].toLowerCase()) ? 1 : 0;
        int rating_disabled = Boolean.getBoolean(video[13].toLowerCase()) ? 1 : 0;
        int video_error_or_remove = Boolean.getBoolean(video[14].toLowerCase()) ? 1 : 0;
        String descripcion = video[15];
        titulo_video = cleanString(titulo_video);
        descripcion = cleanString(descripcion);
        tags = cleanString(tags);
        exeSQLCode("INSERT INTO Dimension_Video (id_video, titulo, descripcion, tags, comments_disabled, retings_disabled, video_error_or_removed, thumbnail_link) VALUES ( \"" + id_video + "\",\"" + titulo_video + "\",\"" + descripcion + "\",\"" + tags + "\",\"" + comments_disabled + "\",\"" + rating_disabled + "\",\"" + video_error_or_remove + "\",\"" + thumbnail_link + "\");");
    }

    public void insertTiempo(String[] video, int id_tiempo) throws ParseException, SQLException {
        LocalDateTime fecha_trend = new SimpleDateFormat("yy.dd.MM").parse(video[1]).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        String fPubli = video[5].replace('T', '-');
        LocalDateTime fecha_publi = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss").parse(fPubli).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        String hora_publi = fecha_publi.getHour() + ":" + fecha_publi.getMinute() + ":" + fecha_publi.getSecond();
        exeSQLCode("INSERT INTO Dimension_Tiempo (id_tiempo, fecha_publi, año_publi, mes_publi, dia_publi, hora_publi, fecha_trend, año_trend, mes_trend, dia_trend) VALUES (" + id_tiempo + ",'" + fecha_publi.toString() + "'," + fecha_publi.getYear() + "," + fecha_publi.getMonthValue() + "," + fecha_publi.getDayOfMonth() + ",'" + hora_publi + "','" + fecha_trend.toString() + "'," + fecha_trend.getYear() + "," + fecha_trend.getMonthValue() + "," + fecha_trend.getDayOfMonth() + ");");
    }

    public void insertHecho(String[] video, String pais, int id) throws SQLException {
        String id_video = video[0];
        String titulo_canal = video[3];
        String categoria = video[4];
        int visitas = Integer.parseInt(video[7]);
        int likes = Integer.parseInt(video[8]);
        int dislikes = Integer.parseInt(video[9]);
        int num_comentarios = Integer.parseInt(video[10]);
        exeSQLCode("INSERT INTO Hechos (id_video, titulo_canal, nombre_categoria, id_tiempo, pais, visitas, likes, dislikes, num_comentarios) VALUES (\"" + id_video + "\",\"" + titulo_canal + "\",\"" + categoria + "\"," + id + ",\"" + pais + "\"," + visitas + "," + likes + "," + dislikes + "," + num_comentarios + ");");
        System.out.println("Inserted video with id: " + id_video);
    }

    public String cleanString(String cadena) {
        cadena = cadena.replace("'", " ");
        cadena = cadena.replace("\"", " ");
        String regex = "[^\\p{L}\\p{N}\\p{P}\\p{Z}]";
        Pattern pattern = Pattern.compile(
                regex,
                Pattern.UNICODE_CHARACTER_CLASS);
        Matcher matcher = pattern.matcher(cadena);
        return matcher.replaceAll("");
    }

}
