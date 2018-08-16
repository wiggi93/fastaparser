package models;

import java.io.Serializable;




public class FastaObject implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String giAccession;
	public String refAccession;
	public String description;
	public String taxonomy;
	public String seq;
	public String jsonString;

        
    public FastaObject(){
    		this.giAccession=null;
            this.refAccession=null;
    		this.description=null;
    		this.taxonomy=null;
            this.seq=null;
    }

	public FastaObject(String meta, String seq) {
		
		this.seq=seq;
		String[] parts1 = meta.split("[|]");
		this.giAccession=parts1[1];
		this.refAccession=parts1[3];
		this.description=parts1[4];
		String[] parts2 = meta.split("\\[");
		String[] parts3 = parts2[1].split("\\]");
		this.taxonomy=parts3[0];
		
	}

	public String getgiAccession() {
		return giAccession;
	}

	public void setgiAccession(String giAccession) {
		this.giAccession = giAccession;
	}

	public String getrefAccession() {
		return refAccession;
	}

	public void setAccession(String accession) {
		this.refAccession = accession;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getseq() {
		return seq;
	}

	public void setseq(String seq) {
		this.seq = seq;
	}

	@Override
	public String toString() {
		return "FastaObject [giAccession=" + giAccession + ", refAccession=" + refAccession + ", description="
				+ description + ", taxonomy=" + taxonomy + ", seq=" + seq + "]";
	}
	

}