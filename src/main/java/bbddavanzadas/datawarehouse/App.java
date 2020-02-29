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
	static ArrayList<String> video_ids = new ArrayList<String>();

    public static void main(String[] args) throws SQLException, ClassNotFoundException, ParseException {
        DBBroker broker = new DBBroker();
        broker.createTables();
        parsear("data/CAvideos.csv", "Canadá", broker);
        parsear("data/FRvideos.csv", "Francia",broker);
        parsear("data/MXvideos.csv", "México",broker);
        parsear("data/USvideos.csv", "USA",broker);
        broker.closeConnection();
    }

    public static void parsear(String csvFile, String pais, DBBroker broker) throws SQLException, ParseException {
        
        CSVReader reader;
        try {
            reader = new CSVReader(new FileReader(csvFile));
            reader.readNext();
            String[] line;
            
            while ((line = reader.readNext()) != null) {
                if (!estaRepetido(line[0], video_ids)) {
                    broker.insertVideo(line);
                    video_ids.add(line[0]);
                }
                broker.insertTiempo(line, id);
                broker.insertHecho(line, pais, id);
                id++;
            }
        } catch (IOException e) {
            System.out.print(e);
        } catch (CsvValidationException e) {
            System.out.print(e);
        }
    }

    public static boolean estaRepetido(String id, ArrayList<String> ids) {
        for (String siguiente : ids) {
            if (id.equals(siguiente)) {
                return true;
            }
        }
        return false;
    }

}
