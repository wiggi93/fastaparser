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

	public Session getSession() {
		return this.session;
	}

	public void close() {
		session.close();
		cluster.close();
	}

	public void writeObjectToProtein(String seq, UUID prot_ID) {

		StringBuilder sb = new StringBuilder("INSERT INTO ").append("polyglot_persistence.protein")
				.append(" (prot_id, prot_seq)").append("VALUES (").append(prot_ID + ", " + "'" + seq).append("');");

		String query = sb.toString();
		session.execute(query);
	}

	public void insertObjectToProtein(UUID prot_ID, String meta, UUID fastaID) {

		List<String> list = new LinkedList<String>();
		list.add(meta);
		Statement st = new SimpleStatement(
				"INSERT INTO polyglot_persistence.protein (prot_id, \"" + fastaID + "\") VALUES (" + prot_ID + ", ?);",
				list);
		session.execute(st);

	}

	public void insertListToProtein(UUID prot_ID, ArrayList<String> list, UUID fastaID) {

		Statement st = new SimpleStatement(
				"INSERT INTO polyglot_persistence.protein (prot_id, \"" + fastaID + "\") VALUES (" + prot_ID + ", ?);",
				list);
		session.execute(st);
	}

	public void writeObjectToProtein_Redundancy(String seq, UUID prot_ID) {

		StringBuilder sb = new StringBuilder("INSERT INTO ").append("polyglot_persistence.protein_redundancy")
				.append(" (prot_seq, prot_id) ").append("VALUES (").append("'" + seq + "', " + prot_ID + " );");

		String query = sb.toString();
		session.execute(query);
	}

	public void createNewColumn(UUID fastaID) {
		String query = "ALTER TABLE polyglot_persistence.protein ADD \"" + fastaID.toString() + "\" SET<TEXT>;";
		session.execute(query);
		query = "ALTER TABLE polyglot_persistence.peptide ADD \"" + fastaID.toString() + "\" SET<UUID>;";
		session.execute(query);
	}

	public void createProteinTable() {
		session.execute(
				"CREATE TABLE IF NOT EXISTS polyglot_persistence.protein (prot_id UUID, prot_seq TEXT, PRIMARY KEY((prot_id)));");
	}
	
	public void createPeptideTable() {
		session.execute(
				"CREATE TABLE IF NOT EXISTS polyglot_persistence.peptide (pep_seq TEXT, PRIMARY KEY((pep_seq)));");
	}

	public void createProtein_RedundancyTable() {
		session.execute(
				"CREATE TABLE IF NOT EXISTS polyglot_persistence.protein_redundancy (prot_id UUID, prot_seq TEXT, PRIMARY KEY(prot_seq));");
	}

	public ResultSet selectProt_Redundancy(String seq) {

		return session
				.execute("SELECT prot_id FROM polyglot_persistence.protein_redundancy WHERE prot_seq= '" + seq + "' ;");
	}

	public void writeMapToPeptideTable(HashMap<String, Set<UUID>> mapCollectDuplicatePeptides, UUID fastaID) {
		for (Entry<String, Set<UUID>> entry : mapCollectDuplicatePeptides.entrySet()) {
			Statement st = new SimpleStatement(
					"INSERT INTO polyglot_persistence.peptide (pep_seq, \"" + fastaID + "\") VALUES ('" + entry.getKey() + "', ?);",
					entry.getValue());
			session.execute(st);
		}
	}

}
