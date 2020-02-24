package bbddavanzadas.datawarehouse;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class App {

	public static void main(String[] args) {
		Connection con = DBBroker.connect();
		DBBroker.createTables();
	}

	public static void parsear() {
		String csvFile = "data/CAvideos.csv";
		CSVReader reader;
		try {
			reader = new CSVReader(new FileReader(csvFile));
			String[] headLine = reader.readNext();
			String[] line;
			while ((line = reader.readNext()) != null) {
				for (int i = 0; i < line.length; i++) {
					System.out.print(headLine[i] + ": " + line[i] + "; ");
				}	
				System.out.println();
			}
		} catch (IOException e) {
			System.out.print(e);
		} catch (CsvValidationException e) {
			System.out.print(e);
		}
	}

}
