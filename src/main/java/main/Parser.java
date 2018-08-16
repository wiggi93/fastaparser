package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;
import com.google.gson.Gson;

import cassandra.CassandraConnection;
import models.FastaObject;

public class Parser {

	public static void main(String[] args) {
	
		CassandraConnection connection = new CassandraConnection();
		connection.connect();
		connection.createTable();
      
		UUID fastaID = UUIDs.timeBased();
		connection.createNewColumn(fastaID);
		
		Gson gson = new Gson();
		
		HashMap<String, HashSet<String>> sequences= new HashMap<String, HashSet<String>>();
		
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
		int count = 0;
		
		
		try {
			while((temp=br.readLine())!=null) {
				
				if (temp.startsWith(">")) {
					
					if (!firstLine) {
						count++;
						
						if (sequences.containsKey(object.seq)) {
							sequences.get(object.seq).add(gson.toJson(object));
							for (String o : sequences.get(object.seq)) {
								System.out.println(o);
							}
						} else {
							HashSet<String> set = new HashSet<String>();
							set.add(gson.toJson(object));
							sequences.put(object.seq, set);
						}
					
					}
					object = new FastaObject(temp, "");
					firstLine = false;
					
				} 
				else {
					object.setseq(object.getseq() + temp.trim());
				}
				
			}
		connection.writeListToCassandra(sequences, fastaID);
		System.out.println(sequences.size());
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		connection.close();
	}

		
		

	}
