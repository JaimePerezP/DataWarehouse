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

import com.vdurmont.emoji.Emoji;
import com.vdurmont.emoji.EmojiParser;

import emoji4j.EmojiUtils;

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
        System.out.println(code);
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
    
    private static boolean isEmoji(String message){
        return message.matches("(?:[\uD83C\uDF00-\uD83D\uDDFF]|[\uD83E\uDD00-\uD83E\uDDFF]|" +
                "[\uD83D\uDE00-\uD83D\uDE4F]|[\uD83D\uDE80-\uD83D\uDEFF]|" +
                "[\u2600-\u26FF]\uFE0F?|[\u2700-\u27BF]\uFE0F?|\u24C2\uFE0F?|" +
                "[\uD83C\uDDE6-\uD83C\uDDFF]{1,2}|" +
                "[\uD83C\uDD70\uD83C\uDD71\uD83C\uDD7E\uD83C\uDD7F\uD83C\uDD8E\uD83C\uDD91-\uD83C\uDD9A]\uFE0F?|" +
                "[\u0023\u002A\u0030-\u0039]\uFE0F?\u20E3|[\u2194-\u2199\u21A9-\u21AA]\uFE0F?|[\u2B05-\u2B07\u2B1B\u2B1C\u2B50\u2B55]\uFE0F?|" +
                "[\u2934\u2935]\uFE0F?|[\u3030\u303D]\uFE0F?|[\u3297\u3299]\uFE0F?|" +
                "[\uD83C\uDE01\uD83C\uDE02\uD83C\uDE1A\uD83C\uDE2F\uD83C\uDE32-\uD83C\uDE3A\uD83C\uDE50\uD83C\uDE51]\uFE0F?|" +
                "[\u203C\u2049]\uFE0F?|[\u25AA\u25AB\u25B6\u25C0\u25FB-\u25FE]\uFE0F?|" +
                "[\u00A9\u00AE]\uFE0F?|[\u2122\u2139]\uFE0F?|\uD83C\uDC04\uFE0F?|\uD83C\uDCCF\uFE0F?|" +
                "[\u231A\u231B\u2328\u23CF\u23E9-\u23F3\u23F8-\u23FA]\uFE0F?)+");
    }

    public void insertVideo(String[] video) throws SQLException {
        String id_video = video[0];
        String titulo_video = video[2];
        String tags = video[6];
        String thumbnail_link = video[11];
        int comments_disabled = Boolean.getBoolean(video[12].toLowerCase()) ? 1:0;
        int rating_disabled = Boolean.getBoolean(video[13].toLowerCase()) ? 1:0;
        int video_error_or_remove = Boolean.getBoolean(video[14].toLowerCase()) ? 1:0;
        String descripcion = video[15];
        System.out.println(id_video);
        System.out.println(titulo_video);
        System.out.println(tags);
        System.out.println(thumbnail_link);
        System.out.println(comments_disabled);
        System.out.println(rating_disabled);
        System.out.println(video_error_or_remove);
        System.out.println(descripcion);
//        System.out.println("INSERT INTO Dimension_Video (id_video, titulo, descripcion, tags, comments_disabled, ratings_disabled, video_error_or_removed, thumbnail_link) VALUES ('" + id_video + "','" + titulo_video + "','" + descripcion + "','" + tags + "'," + comments_disabled + "," + rating_disabled + "," + video_error_or_remove + ",'" + thumbnail_link + "');");
        titulo_video = titulo_video.replace("'", " ");
        descripcion = descripcion.replace("'", " ");
        titulo_video = titulo_video.replace("\"", " ");
        descripcion = descripcion.replace("\"", " ");
        descripcion = EmojiUtils.shortCodify(descripcion);
        descripcion = EmojiParser.removeAllEmojis(descripcion);
        titulo_video = EmojiParser.removeAllEmojis(titulo_video);
        tags = EmojiParser.removeAllEmojis(tags);
        String regex = "[^\\p{L}\\p{N}\\p{P}\\p{Z}]";
        Pattern pattern = Pattern.compile(
          regex, 
          Pattern.UNICODE_CHARACTER_CLASS);
        Matcher matcher = pattern.matcher(descripcion);
        Matcher matcher2 = pattern.matcher(titulo_video);
        Matcher matcher3 = pattern.matcher(tags);
        tags = matcher3.replaceAll("");
        titulo_video = matcher2.replaceAll("");
        descripcion = matcher.replaceAll("");
        tags = tags.replace("'", " ");
        tags = tags.replace("\"", " ");
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
    }

}
