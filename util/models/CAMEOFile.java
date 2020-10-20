package util.models;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CAMEOFile {

	public static final String fileName = "CAMEO.eventcodes.txt";
    private static final Map<String, String> codeAsKey;
    private static final Map<String, String> valueAsKey;
    
    static
    {
        codeAsKey= new HashMap<String, String>();
        valueAsKey = new HashMap<String, String>();
        
        boolean verbose =false;
        String delim = "	";
        BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(fileName));
			 while(true) {
		            final String line = br.readLine();
		            if(line == null) {
		                break;
		            }
		            else if(line.startsWith("CAMEOEVENTCODE") || line.startsWith("%") || line.startsWith("//")) { //comment
		                if(verbose) {
		                    System.err.println("The following line was ignored during loading a graph:");
		                    System.err.println(line);
		                }
		                continue;
		            }
		            else {
		                String[] tokens = line.split(delim);
		                String id = tokens[0];
		                String val = tokens[1];
		                codeAsKey.put(id,val);
		                valueAsKey.put(val,id);
		            }
		        }
			 System.out.println("counts: "+ codeAsKey.size());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
    }
    
    
    /**
     * Takes the code id and returns the value of the code
     */
    public static String getTileFromId(String id) {
       return codeAsKey.get(id);
    }

    /**
     * Takes a value as parameter and returns all codes containing that value
     */
    public static List<String> getCode(String value) {
        List<String> allCodes = new ArrayList<>();
        for (String s : valueAsKey.keySet()) {
            if (s.contains(value.toLowerCase())) {
                allCodes.add(valueAsKey.get(s));
            }
        }

        return allCodes;
    }
    
}
