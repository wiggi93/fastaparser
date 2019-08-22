package cassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

public class CassandraConnection {

    private Cluster cluster;

    private Session session;

    public void connect() {
        Builder b = Cluster.builder().addContactPoint("127.0.0.1");
        b.withPort(9042);
        cluster = b.build();
        session = cluster.connect();
    }

    public void close() {
        session.close();
        cluster.close();
    }


    /**
     * Writes protein entry to Cassandra protein table
     *
     * @param seq       peptide sequence of protein
     * @param prot_ID   UUID of protein
     */
    public void writeObjectToProtein(String seq, UUID prot_ID) {

        StringBuilder sb = new StringBuilder("INSERT INTO ").append("polyglot_persistence.protein")
                .append(" (prot_id, prot_seq)").append("VALUES (").append(prot_ID + ", " + "'" + seq).append("');");

        String query = sb.toString();
        session.execute(query);
    }

    /**
     * Writes single protein entry to cassandra protein table for current fasta file.
     * Meta data is written in the column of fasta file ID as a list.
     *
     * @param prot_ID   UUID of protein
     * @param meta      meta data of fasta entry as string
     * @param fastaID   UUID of current fasta file
     */
    public void insertObjectToProtein(UUID prot_ID, String meta, UUID fastaID) {

        List<String> list = new LinkedList<>();
        list.add(meta);
        Statement st = new SimpleStatement(
                "INSERT INTO polyglot_persistence.protein (prot_id, \"" + fastaID + "\") VALUES (" + prot_ID + ", ?);",
                list);
        session.execute(st);

    }

    /**
     * Writes a protein entry to cassandra protein table containing multiple meta data entries as list.
     * Meta data list is written to fasta ID column.
     *
     * @param prot_ID   UUID of protein
     * @param list      list of meta data for multiple proteins
     * @param fastaID   UUID of current fasta file
     */
    public void insertListToProtein(UUID prot_ID, ArrayList<String> list, UUID fastaID) {

        Statement st = new SimpleStatement(
                "INSERT INTO polyglot_persistence.protein (prot_id, \"" + fastaID + "\") VALUES (" + prot_ID + ", ?);",
                list);
        session.execute(st);
    }

    /**
     * Writes protein to protein redundancy table
     *
     * @param seq       peptide sequence of protein
     * @param prot_ID   UUID of protein
     */
    public void writeObjectToProtein_Redundancy(String seq, UUID prot_ID) {

        StringBuilder sb = new StringBuilder("INSERT INTO ").append("polyglot_persistence.protein_redundancy")
                .append(" (prot_seq, prot_id) ").append("VALUES (").append("'" + seq + "', " + prot_ID + " );");

        String query = sb.toString();
        session.execute(query);
    }

    /**
     * Creates new column for a new fasta file in Cassandra protein table and peptide table
     *
     * @param fastaID   UUID of current fasta file
     */
    public void createNewColumn(UUID fastaID) {
        String query = "ALTER TABLE polyglot_persistence.protein ADD \"" + fastaID.toString() + "\" SET<TEXT>;";
        session.execute(query);
        query = "ALTER TABLE polyglot_persistence.peptide ADD \"" + fastaID.toString() + "\" SET<UUID>;";
        session.execute(query);
    }

    /**
     * Creates keyspace in Cassandra
     *
     * @param keyspaceName  Name of the keyspace that is created
     */
    public void createKeyspace(String keyspaceName) {
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspaceName + " WITH replication = " +
                "{'class':'SimpleStrategy', 'replication_factor' : 1};");
    }

    /**
     * Creates protein table
     */
    public void createProteinTable() {
        session.execute(
                "CREATE TABLE IF NOT EXISTS polyglot_persistence.protein (prot_id UUID, prot_seq TEXT, PRIMARY KEY((prot_id)));");
    }

    /**
     * Creates peptide table
     */
    public void createPeptideTable() {
        session.execute(
                "CREATE TABLE IF NOT EXISTS polyglot_persistence.peptide (pep_seq TEXT, PRIMARY KEY((pep_seq)));");
    }

    /**
     * creates protein redundancy table
     */
    public void createProtein_RedundancyTable() {
        session.execute(
                "CREATE TABLE IF NOT EXISTS polyglot_persistence.protein_redundancy (prot_id UUID, prot_seq TEXT, PRIMARY KEY(prot_seq));");
    }

    /**
     * Gets the entry from protein redundancy table for specified protein sequence
     *
     * @param seq   protein sequence
     * @return      protein redundancy entry
     */
    public ResultSet selectProt_Redundancy(String seq) {

        return session
                .execute("SELECT prot_id FROM polyglot_persistence.protein_redundancy WHERE prot_seq= '" + seq + "' ;");
    }

    /**
     * Writes a map of peptides to Cassandra peptide table
     *
     * @param mapCollectDuplicatePeptides   Map of peptides containing a set of prot_IDs to the protein it belongs to
     * @param fastaID                       UUID of current fasta file
     */
    public void writeMapToPeptideTable(HashMap<String, Set<UUID>> mapCollectDuplicatePeptides, UUID fastaID) {
        for (Entry<String, Set<UUID>> entry : mapCollectDuplicatePeptides.entrySet()) {
            Statement st = new SimpleStatement(
                    "INSERT INTO polyglot_persistence.peptide (pep_seq, \"" + fastaID + "\") VALUES ('" + entry.getKey() + "', ?);",
                    entry.getValue());
            session.execute(st);
        }
    }

}
