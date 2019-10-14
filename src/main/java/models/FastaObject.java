package models;

import java.io.Serializable;

public class FastaObject implements Serializable{

	private static final long serialVersionUID = 1L;
	public String giAccession;
	public String refAccession;
	public String description;
	public String taxonomy;
	public String uniprotAccession;

        
    public FastaObject(){
    		this.giAccession=null;
            this.refAccession=null;
    		this.description=null;
    		this.taxonomy=null;
    		this.uniprotAccession=null;
    }

	@Override
	public String toString() {
		return "FastaObject [giAccession=" + giAccession + ", refAccession=" + refAccession + ", description="
				+ description + ", taxonomy=" + taxonomy + "]";
	}
	

}