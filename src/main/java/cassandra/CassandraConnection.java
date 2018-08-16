package cassandra;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.gson.Gson;

import models.FastaObject;

public class CassandraConnection{
	
	private Cluster cluster;
	 
    private Session session;
    
    private Gson gson = new Gson();
 
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
    
    
    public void writeObjectToCassandra(FastaObject object, UUID fastaID) {
    	
    	StringBuilder sb = new StringBuilder("INSERT INTO ")
    		      .append("polyglot_persistence.protein").append(" (prot_seq, \"" + fastaID + "\") ")
    		      .append("VALUES (").append("'"+object.seq+"'"+", "+"'"+gson.toJson(object)).append("');");
    	
    		    String query = sb.toString();
    		    session.execute(query);
    }
    
    public void writeListToCassandra(HashMap<String ,HashSet<String>> lists, UUID fastaID) {
    	
    	for(Map.Entry<String, HashSet<String>> entry : lists.entrySet()) {
    	    writeEntryToCassandra(entry.getKey(), entry.getValue(), fastaID);
    	}
    }

	public void writeEntryToCassandra(String prot_seq, HashSet<String> list, UUID fastaID) {
    	
    	Statement st = new SimpleStatement("INSERT INTO polyglot_persistence.protein (prot_seq, \""+fastaID+"\") VALUES ('"+prot_seq+"', ?);", list); 
    	session.execute(st);

    }
    	

	public void createNewColumn(UUID fastaID) {
		String query = "ALTER TABLE polyglot_persistence.protein ADD \"" + fastaID.toString() + "\" SET<TEXT>;" ;
		session.execute(query);
	}
	
	public void createTable() {
		session.execute("CREATE TABLE IF NOT EXISTS polyglot_persistence.protein (prot_seq TEXT, PRIMARY KEY((prot_seq)));");
	}

	
}


