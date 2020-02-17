package bbddavanzadas.datawarehouse;

import java.io.FileReader;
import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App {
		
	public static void main(String[] args) {
		String csvFile = "C:\\Users\\ulabjpp\\Desktop\\CAvideos.csv";
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
		} catch (IOException | CsvValidationException e) {
			System.out.print(e);
		}
	}

}
