package bbddavanzadas.datawarehouse;

import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.text.ParseException;
import java.util.ArrayList;

public class App {
	static int id = 0;
	static ArrayList<String> video_ids = new ArrayList<>();

	public static void main(String[] args) throws SQLException, ClassNotFoundException, ParseException {
		DBBroker broker = new DBBroker();
		broker.createTables();
		parsear("data/CAvideos.csv", "Canadá", broker);
		parsear("data/FRvideos.csv", "Francia", broker);
		parsear("data/MXvideos.csv", "México", broker);
		parsear("data/USvideos.csv", "USA", broker);
		parsear("data/GBvideos.csv", "Gran Bretaña", broker);
		parsear("data/INvideos.csv", "India", broker);
		parsear("data/JPvideos.csv", "Japón", broker);
		broker.closeConnection();
	}

	public static void parsear(String csvFile, String pais, DBBroker broker) throws SQLException, ParseException {
		CSVReader reader;
		ArrayList<String[]> rows = new ArrayList<>();
		try {
			reader = new CSVReader(new FileReader(csvFile));
			reader.readNext();
			String[] line;
			while ((line = reader.readNext()) != null) {
				rows.add(line);
			}
			for (int i = 0; i < rows.size(); i++) {
				if (!video_ids.contains(rows.get(i)[0])) {
					broker.insertVideo(rows.get(i));
					video_ids.add(rows.get(i)[0]);
				}
				broker.insertTiempo(rows.get(i), id);
				broker.insertHecho(rows.get(i), pais, id);
				id++;
			}
		} catch (IOException e) {
			System.out.print(e);
		} catch (CsvValidationException e) {
			System.out.print(e);
		}
	}

}
