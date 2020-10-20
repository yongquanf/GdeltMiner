package util.csv;

import java.io.File;
import java.io.InputStream;

public class GdeltCSVParser {

	 private final CsvProcessor csvProcessor;
	 
	 public GdeltCSVParser(){
		 this.csvProcessor = new CsvProcessor();
	 }
	/**
     * Parses a GDELT CSV file.
     *
     * @param file The CSV file.
     * @return the GDELT CSV records as POJOs.
     */
    public GDELTReturnResult parseCsv(File file)
    {
        return csvProcessor.processCSV(file);
    }

    // TODO make public once the test passes
    GDELTReturnResult parseCsv(InputStream inputStream)
    {
        return csvProcessor.processCSV(inputStream);
    }
}
