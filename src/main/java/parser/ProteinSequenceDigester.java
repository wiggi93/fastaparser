package parser;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.expasy.mzjava.proteomics.mol.Peptide;
import org.expasy.mzjava.proteomics.mol.Protein;
import org.expasy.mzjava.proteomics.mol.digest.Protease;
import org.expasy.mzjava.proteomics.mol.digest.ProteinDigester;

public class ProteinSequenceDigester {


	public Set<String> digestAndFragementProtein(String protAcession, String protSequence) {
		
        ProteinDigester digester = new ProteinDigester.Builder(Protease.TRYPSIN).build();
 
        Protein prot = new Protein(protAcession, protSequence);
 
        List<Peptide> list = digester.digest(prot);
        Set<String> resultPeptides = new HashSet<String>();
        
        for (Peptide pep : list) {
 
        	if(pep.toString().length()>5 && pep.toString().length()<30)
            resultPeptides.add(pep.toString());
 
        }
        
        return resultPeptides;
    }
	
}

