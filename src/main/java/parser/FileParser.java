package parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

public class FileParser implements Iterator<String>{

	private File fasta;
	private BufferedReader br;
	private String nextLine;
	private Boolean entryStart=true;
	
	public FileParser(String path) {
		
		fasta = new File(path);
		try {
            br = new BufferedReader(new FileReader(fasta));
            nextLine = br.readLine();
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
	}
		
	public boolean hasNext() {
		return nextLine!=null;
	}

	public String next() {
		
		try {
			// TODO StringBuilder statt String
			String result = "";
			
			if (nextLine==null) {
                br.close();
            }
			
			if ((nextLine != null)) {
				
				do {
					result=result+nextLine;
					
					if (entryStart) {
						result=result+"$";
					}
					entryStart=false;
					nextLine=br.readLine();
					
					if (nextLine==null) {
						entryStart=true;
						return result;
					}
 				} 
				while (!nextLine.startsWith(">"));
			}
			entryStart=true;
			return result;
	
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
			}
	}

	public void reset() {
		
		try {
            br = new BufferedReader(new FileReader(fasta));
            nextLine = br.readLine();
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
	}
	
}

