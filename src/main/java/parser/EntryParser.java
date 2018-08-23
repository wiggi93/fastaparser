package parser;

import models.FastaObject;

public class EntryParser {
	
	public String getSequence(String entry) {
		String[] parts = entry.split("[$]");
		return parts[1];
	}

	public FastaObject getFastaObject(String entry) {
		FastaObject object = new FastaObject();
		String[] parts1 = entry.split("[|]");
		object.giAccession=parts1[1];
		object.refAccession=parts1[3];
		String[] parts2 = parts1[4].split("[$]");
		object.description=parts2[0];
		String[] parts3 = entry.split("\\[");
		String[] parts4 = parts3[1].split("\\]");
		object.taxonomy=parts4[0];
		
		return object;
		
	}
}
