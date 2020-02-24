package bbddavanzadas.datawarehouse;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class App {

	public static void main(String[] args) {
		Connection con = DBBroker.connect();
		DBBroker.createTables();
		try {
			DBBroker.insert();
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		parsear();
	}

	public static void parsear() {
		String csvFile = "data/CAvideos.csv";
		CSVReader reader;
		try {
			reader = new CSVReader(new FileReader(csvFile));
			String[] headLine = reader.readNext();
			String[] line;
			String id_video, title, channel_title, description, thumbal_link, retings_disabled, 
			comments_disabled, video_error_or_removed, tags, comments_count;
			int id_dates, id_options, id_group, id_statistics, category_id, views, likes, dislikes;
			Date trending_date, publish_time; // Todas estas variables las he creado para el switch y guardar las cosas de cada iteración
			while ((line = reader.readNext()) != null) {
				for (int i = 0; i < line.length; i++) {
					System.out.print(headLine[i] + ": " + line[i] + "; ");
					switch (headLine[i]) {
					case "video_id":
						id_video = line[i];
						break;
					case "trending_date":
//						trending_date = line[i];  // Aquí está el problema que te digo en los comentarios
						break;
					/* Aquí todo lo demás. Te dejo esta idea a ver si la ves porque no sé.
					 * 
					 * Además line es un vector de String y habría que parsearlo a Date
					 * ¿Igual las variables de tipo Date podrían ser String y parsearlas al meterlas en la BD?
					 * 
					 * Es que claro, para hacer el insert necesitamos valores que no están necesariamente seguidos,
					 * por eso he creado tantas variables. A ver qué opinas
					 * 
					 * Un saludo.*/

					default:
						break;
					}
				}
				System.out.println();
				System.out.println();
			}
		} catch (IOException e) {
			System.out.print(e);
		} catch (CsvValidationException e) {
			System.out.print(e);
		}
	}

}
