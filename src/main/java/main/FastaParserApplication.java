package main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;
import com.google.gson.Gson;

import cassandra.CassandraConnection;
import models.FastaObject;
import parser.EntryParser;
import parser.FileParser;

public class FastaParserApplication {

	private CassandraConnection connection;
	
	private UUID fastaID;

	private Gson gson = new Gson();
	private HashMap<UUID, Long> map1 = new HashMap<UUID, Long>();
	private HashMap<UUID, ArrayList<String>> map2 = new HashMap<UUID, ArrayList<String>>();

	
	public static void main(String[] args) {
		new FastaParserApplication();
	}
	
	public FastaParserApplication() {
		
		FileParser fp = new FileParser("/Users/philip/Desktop/arbeit/WorkspaceSpeL/NCBI_Homo_Sapiens_refSeq.fasta");
		EntryParser ep = new EntryParser();
		
		fastaID = UUIDs.timeBased();

		connection = new CassandraConnection();
		connection.connect();
		connection.createProteinTable();
		connection.createProtein_RedundancyTable();
		connection.createNewColumn(fastaID);
		
		while(fp.hasNext()) {
			checkRedundancy(ep.getSequence(fp.next()));
		}
		
		fp.reset();
		
		while(fp.hasNext()) {
			String entry = fp.next();
			updateProteinTable(ep.getSequence(entry), ep.getFastaObject(entry));
		}
		
		connection.close();
	}
		

	public void checkRedundancy(String seq) {
		
		ResultSet rs = connection.selectProt_Redundancy(seq);
		
		//if object doesnt exist in cassandra --> write to cassandra protein_redundancy
		if (rs.isExhausted()) {
			UUID prot_ID = UUIDs.timeBased();
			connection.writeObjectToProtein_Redundancy(seq, prot_ID);
			connection.writeObjectToProtein(seq, prot_ID);
			long amount = 1;
			map1.put(prot_ID, amount);
		}
		
		//if object is already in cassandra --> get existing UUID and map to fastaIndex
		else{
			Row result = rs.one();
			UUID prot_ID = result.getUUID(0);
			
			//if object is already in map --> add object to array at key for prot_id
			if (map1.containsKey(prot_ID)) {
				map1.put(prot_ID, map1.get(prot_ID)+1);
				
			} // if object is already in cassandra but not in map, create new map entry for existing prot_ID
			else {
				map1.put(prot_ID, (long) 1);
			}
		}
		
	}
	
	public void updateProteinTable(String seq, FastaObject object) {
		
		UUID prot_ID = connection.selectProt_Redundancy(seq).one().getUUID(0);
		
		if (map1.get(prot_ID).longValue()>1) {
			
			if (map2.containsKey(prot_ID)) {
				System.out.println(prot_ID);
				map2.get(prot_ID).add(gson.toJson(object));
			}
			else {
				ArrayList<String> list= new ArrayList<String>();
				list.add(gson.toJson(object));
				map2.put(prot_ID, list);
			}
			
			map1.put(prot_ID, map1.get(prot_ID)-1);
		}
		
		if (map1.get(prot_ID).longValue()==1) {
			
			if (map2.containsKey(prot_ID)) {
				connection.insertListToProtein(prot_ID, map2.get(prot_ID), fastaID);
			}
			else {
				connection.insertObjectToProtein(prot_ID, gson.toJson(object), fastaID);;
			}
		}
		
	}

}