package bbddavanzadas.datawarehouse;

import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.text.ParseException;

public class App {

    public static void main(String[] args) throws SQLException, ClassNotFoundException, ParseException {
        DBBroker broker = new DBBroker();
        broker.createTables();
        parsear(broker);
        broker.closeConnection();
    }

    public static void parsear(DBBroker broker) throws SQLException, ParseException {
        String csvFile = "data/CAvideos.csv";
        CSVReader reader;
        try {
            reader = new CSVReader(new FileReader(csvFile));
            reader.readNext();
            String[] line;
            int id = 0;
            while ((line = reader.readNext()) != null) {
                broker.insertVideo(line, id);
                broker.insertTiempo(line, id);
                broker.insertHecho(line, "Canadá", id);
                id++;
            }
        } catch (IOException e) {
            System.out.print(e);
        } catch (CsvValidationException e) {
            System.out.print(e);
        }
    }

}
