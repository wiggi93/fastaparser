package parser;

import models.FastaObject;

public class EntryParser {

	/**
	 * extracts the peptide sequence of a protein entry in fasta file
	 *
	 * @param entry  	one protein entry of the fasta file
	 * @return 			peptide sequence as string
	 */
	public String getSequence(String entry) {
		String[] parts = entry.split("[$]");
		return parts[1];
	}

	/**
	 * creates FastaObject from an entry of the fasta file
	 *
	 * @param entry  	one protein entry of the fasta file
	 * @return 			entry as FastaObject
	 */
	public FastaObject getFastaObject(String entry) {
		FastaObject object = new FastaObject();
		String[] parts1 = entry.split("[|]");
		if (parts1[0].contains("ref")) {
			
		object.giAccession=parts1[1];
		object.refAccession=parts1[3];
		String[] parts2 = parts1[4].split("[$]");
		object.description=parts2[0];
		
		String[] parts3 = entry.split("\\[");
		String[] parts4 = parts3[1].split("\\]");
		object.taxonomy=parts4[0];
		}
		else {
			object.uniprotAccession=parts1[1];
			object.description=parts1[2];
			object.taxonomy="homo sapiens";
		}
		return object;
		
	}
}
