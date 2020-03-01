package bbddavanzadas.datawarehouse;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONObject;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class App {
	static int id = 0;
	static ArrayList<String> video_ids = new ArrayList<>();

	public static void main(String[] args) throws SQLException, ClassNotFoundException, ParseException, IOException {
		DBBroker broker = new DBBroker();
		broker.createTables();
		parsear("data/CAvideos.csv", "Canadá", broker, getJSONCateogrias("data/CA_category_id.json"));
		parsear("data/FRvideos.csv", "Francia", broker, getJSONCateogrias("data/FR_category_id.json"));
		parsear("data/MXvideos.csv", "México", broker, getJSONCateogrias("data/MX_category_id.json"));
		parsear("data/USvideos.csv", "USA", broker, getJSONCateogrias("data/US_category_id.json"));
		parsear("data/GBvideos.csv", "Gran Bretaña", broker, getJSONCateogrias("data/GB_category_id.json"));
		parsear("data/INvideos.csv", "India", broker, getJSONCateogrias("data/IN_category_id.json"));
		parsear("data/JPvideos.csv", "Japón", broker, getJSONCateogrias("data/JP_category_id.json"));
		broker.closeConnection();
	}

	public static void parsear(String csvFile, String pais, DBBroker broker, JSONObject categorias)
			throws SQLException, ParseException {
		CSVReader reader;
		ArrayList<String[]> rows = new ArrayList<>();
		try {
			reader = new CSVReader(new FileReader(csvFile));
			reader.readNext();
			String[] line;
			while ((line = reader.readNext()) != null) {
				JSONArray cs = categorias.getJSONArray("items");
				for (int i = 0; i < cs.length(); i++) {
					JSONObject categoria = cs.getJSONObject(i);
					if(categoria.getString("id").contentEquals(line[4])) {
						JSONObject snippet = categoria.getJSONObject("snippet");
						line[4] = snippet.getString("title");
					}
				}
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

	public static JSONObject getJSONCateogrias(String ruta) throws IOException {
		String content = new String(Files.readAllBytes(Paths.get(ruta)));
		return new JSONObject(content);
	}

}
