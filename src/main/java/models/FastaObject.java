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
	public String jsonString;
	public String uniprotAccession;

        
    public FastaObject(){
    		this.giAccession=null;
            this.refAccession=null;
    		this.description=null;
    		this.taxonomy=null;
    		this.uniprotAccession=null;
    }

	public String getUniprotAccession() {
		return uniprotAccession;
	}

	public void setUniprotAccession(String uniprotAccession) {
		this.uniprotAccession = uniprotAccession;
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


	@Override
	public String toString() {
		return "FastaObject [giAccession=" + giAccession + ", refAccession=" + refAccession + ", description="
				+ description + ", taxonomy=" + taxonomy + "]";
	}
	

}