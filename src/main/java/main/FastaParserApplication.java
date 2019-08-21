package main;

import cassandra.CassandraConnection;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;
import com.google.gson.Gson;
import models.FastaObject;
import parser.EntryParser;
import parser.FileParser;
import parser.ProteinSequenceDigester;

import java.util.*;

public class FastaParserApplication implements Runnable {

    private CassandraConnection connection;
    private ProteinSequenceDigester digester;
    private UUID fastaID;
    private Gson gson = new Gson();
    private HashMap<String, Set<UUID>> mapCollectDuplicatePeptides = new HashMap<String, Set<UUID>>();
    private HashMap<UUID, Long> mapCountDuplicates = new HashMap<UUID, Long>();
    private HashMap<UUID, ArrayList<String>> mapCollectDuplicateEntries = new HashMap<UUID, ArrayList<String>>();
    private String path;

    public static void main(String[] args) {
        FastaParserApplication application = new FastaParserApplication(
                "src/assets/uniprot-short.fasta");
        application.run();
    }

    public FastaParserApplication(String path) {
        this.path = path;
    }

    public void setUpCassandraConnection() {
        connection = new CassandraConnection();
        connection.connect();
        connection.createKeyspace("polyglot_persistence");
        connection.createProteinTable();
        connection.createProtein_RedundancyTable();
        connection.createPeptideTable();
        connection.createNewColumn(fastaID);
    }

    public UUID checkRedundancy(String seq) {

        ResultSet rs = connection.selectProt_Redundancy(seq);
        UUID prot_ID = null;
        // if object doesnt exist in cassandra --> write to cassandra protein_redundancy
        if (rs.isExhausted()) {
            prot_ID = UUIDs.timeBased();
            connection.writeObjectToProtein_Redundancy(seq, prot_ID);
            connection.writeObjectToProtein(seq, prot_ID);
            long amount = 1;
            mapCountDuplicates.put(prot_ID, amount);
        }

        // if object is already in cassandra --> get existing UUID and map to fastaIndex
        else {
            Row result = rs.one();
            prot_ID = result.getUUID(0);

            // if object is already in map --> add object to array at key for prot_id
            if (mapCountDuplicates.containsKey(prot_ID)) {
                mapCountDuplicates.put(prot_ID, mapCountDuplicates.get(prot_ID) + 1);

            } // if object is already in cassandra but not in map, create new map entry for
            // existing prot_ID
            else {
                mapCountDuplicates.put(prot_ID, (long) 1);
            }
        }
        return prot_ID;

    }

    public void updateProteinTable(String seq, FastaObject object) {

        UUID prot_ID = connection.selectProt_Redundancy(seq).one().getUUID(0);

        if (mapCountDuplicates.get(prot_ID).longValue() > 1) {

            if (mapCollectDuplicateEntries.containsKey(prot_ID)) {
                mapCollectDuplicateEntries.get(prot_ID).add(gson.toJson(object));
            } else {
                ArrayList<String> list = new ArrayList<String>();
                list.add(gson.toJson(object));
                mapCollectDuplicateEntries.put(prot_ID, list);
            }

            mapCountDuplicates.put(prot_ID, mapCountDuplicates.get(prot_ID) - 1);
        }

        if (mapCountDuplicates.get(prot_ID).longValue() == 1) {

            if (mapCollectDuplicateEntries.containsKey(prot_ID)) {
                connection.insertListToProtein(prot_ID, mapCollectDuplicateEntries.get(prot_ID), fastaID);
            } else {
                connection.insertObjectToProtein(prot_ID, gson.toJson(object), fastaID);
                ;
            }
        }

    }

    public void run() {

        FileParser fp = new FileParser(this.path);
        EntryParser ep = new EntryParser();
        digester = new ProteinSequenceDigester();

        fastaID = UUIDs.timeBased();
        UUID prot_ID = null;
        setUpCassandraConnection();

        Set<String> peptideSequences = new HashSet<String>();

        while (fp.hasNext()) {
            peptideSequences.clear();
            String entry = fp.next();
            prot_ID = checkRedundancy(ep.getSequence(entry));
            peptideSequences = digester.digestAndFragementProtein("", ep.getSequence(entry));
            for (String s : peptideSequences) {
                if (mapCollectDuplicatePeptides.containsKey(s)) {
                    mapCollectDuplicatePeptides.get(s).add(prot_ID);
                } else {
                    HashSet<UUID> temp = new HashSet<UUID>();
                    temp.add(prot_ID);
                    mapCollectDuplicatePeptides.put(s, temp);
                }
            }
        }
        connection.writeMapToPeptideTable(mapCollectDuplicatePeptides, fastaID);
        System.out.println("done with PeptideTable");
        fp.reset();
        while (fp.hasNext()) {
            String entry = fp.next();
            updateProteinTable(ep.getSequence(entry), ep.getFastaObject(entry));
        }
        connection.close();
    }

}