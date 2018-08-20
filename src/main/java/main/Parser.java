package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;
import com.google.gson.Gson;

import cassandra.CassandraConnection;
import models.FastaObject;

public class Parser {

	
	
	public static void main(String[] args) {
	
		CassandraConnection connection = new CassandraConnection();
		connection.connect();
		connection.createProteinTable();
		connection.createProtein_RedundancyTable();
      
		UUID fastaID = UUIDs.timeBased();
		connection.createNewColumn(fastaID);
		
		Gson gson = new Gson();
		
		HashMap<UUID, long[]> map = new HashMap<UUID, long[]>();
		
		File test = new File("/Users/philip/Desktop/arbeit/WorkspaceSpeL/NCBI_Homo_Sapiens_refSeq.fasta");
      
		FileReader fr = null;
		try {
			fr = new FileReader(test);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		BufferedReader br = new BufferedReader(fr);
		String temp= "";
		FastaObject object=null;
		boolean firstLine=true;
		long fastaIndex = 0;
		int count = 0;
		
		
		try {
			while((temp=br.readLine())!=null) {
				
				if (temp.startsWith(">")) {
					
					if (!firstLine) {
						count++;
						
						//if object doesnt exist in cassandra --> write to cassandra protein_redundancy
						ResultSet rs = connection.selectProt_Redundancy(object);
						if (rs.isExhausted()) {
							UUID prot_ID = UUIDs.timeBased();
							connection.writeObjectToProtein_Redundancy(object, prot_ID);
							long[] arr = new long[1];
							arr[0] = fastaIndex;
							fastaIndex++;
							map.put(prot_ID, arr);
						}
						
						//if object is already in cassandra --> get existing UUID and map to fastaIndex
						else{
							Row result = rs.one();
							UUID prot_ID = result.getUUID(0);
							long[] arrayOld = map.get(prot_ID);
							long[] arrayNew = new long[arrayOld.length+1];
							System.arraycopy(arrayOld, 0, arrayNew, 0, arrayOld.length);
							arrayNew[arrayNew.length-1] = fastaIndex;
							fastaIndex++;
							map.put(result.getUUID(0), arrayNew);
						}

					
					}
					object = new FastaObject(temp, "");
					firstLine = false;
					
				} 
				else {
					object.setseq(object.getseq() + temp.trim());
				}
				
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		connection.close();
	}
	
	public void parseFile() {
		
	}
	public void checkRedundandy() {
		
	}

}
