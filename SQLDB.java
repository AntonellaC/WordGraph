package wordgraph;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;




public class SQLDB {
	public static void addUWN() throws FileNotFoundException, IOException {
		try (BufferedReader TSVReader = new BufferedReader(new FileReader("res/uwn.tsv"))) {
	        String line = null;
	        while ((line = TSVReader.readLine()) != null) {
	            String[] lineItems = line.split("\t"); //splitting the line and adding its items in String[]
	            System.out.println(Arrays.toString(lineItems));
	            String synset = "", other_lang, rel;
	            Boolean from = true;
	            if (lineItems[0].contains("s/")){
	            	synset = lineItems[0];
	            	other_lang = lineItems[2];
	            }else {
	            	from=false;
	            	synset = lineItems[2];
	            	other_lang = lineItems[0];
	            }
	            rel = lineItems[1];
	            synset = synset.charAt(2)+"#"+synset.substring(3, synset.length());
	            System.out.println(synset);
	            try ( WorldGraph wg = new WorldGraph( "bolt://localhost:7687", "neo4j", "memoria" ) )
		        {
		            Boolean ret = wg.addUWN(synset, rel, other_lang, from);
		        }catch(Exception e){
		        	System.out.println("Errore");
		        	System.out.println(e);
		        }
	            
	        
	        }
	}}
	public static void addSynsets(Connection con, String lang) throws SQLException, IOException, ParseException {
		JSONParser parser = new JSONParser();
		FileReader reader = new FileReader("res/mapping.json");
		JSONObject obj = (JSONObject) parser.parse(reader);
		JSONParser senti_parser = new JSONParser();
		FileReader senti_reader = new FileReader("res/senti.json");
		JSONObject senti_obj = (JSONObject) senti_parser.parse(senti_reader);
		
	    String query = "select id, word, phrase, gloss from "+lang+"_synset";
	    System.out.println(query);
	    try (Statement stmt = con.createStatement()) {
	      ResultSet rs = stmt.executeQuery(query);
	      while (rs.next()) {
	    	  String id = rs.getString("id");
	    	  String new_id = "";
	    	  try{
	    		  new_id = (String) obj.get(id);
	    	  }catch(Exception e) {
	    		  continue;
	    	  }
	    	  if(new_id == null) continue;
	    	  String word = rs.getString("word");
	    	  System.out.println(word);
	    	  String phrase = rs.getString("phrase");
	    	  String gloss = rs.getString("gloss");
	    	  double negScore = -1;
	    	  double objScore = -1;
	    	  double posScore = -1;
	    	  try {
	    		  JSONArray scores = (JSONArray) senti_obj.get(new_id);
	    		  negScore = (double) scores.get(0);
	    		  objScore = (double) scores.get(1);
	    		  posScore = (double) scores.get(2);

 	    	  }catch(Exception e) {}
		        try ( WorldGraph wg = new WorldGraph( "bolt://localhost:7687", "neo4j", "memoria" ) )
		        {
		            Boolean ret = wg.addSynset(new_id, word, phrase, gloss, lang, negScore, objScore, posScore);
		            if (!ret) continue;
		        }catch(Exception e){
		        	System.out.println("Errore addsynset");
		        	System.out.println(e);
		        }
		        String query_lemma = "select id, pos, lemma, is_phrase from "+lang+"_lemma where id='"+id+"'";
		        try (Statement stmt_lemma = con.createStatement()) {
		        	ResultSet rs_lemma = stmt_lemma.executeQuery(query_lemma);
		        	while (rs_lemma.next()) {
		        		String synset_id = rs_lemma.getString("id");
			   	    	String pos = rs_lemma.getString("pos");
			   	    	String lemma = rs_lemma.getString("lemma");
			   	    	String is_phrase = rs_lemma.getString("is_phrase");
			   		    try ( WorldGraph wg = new WorldGraph( "bolt://localhost:7687", "neo4j", "memoria" ) )
			   		    {
			   		        wg.addLemma(new_id, pos, lemma, is_phrase, lang);
			   		    }catch(Exception e){
			   		     	System.out.println("Errore addlemma");
			   		       	System.out.println(e);
			   		    }
		        	}
		        } catch (SQLException e) {
			    	System.out.println(e);
			    }
	      }
	    } catch (SQLException e) {
	    	System.out.println(e);
	    }
	  }
	public static void addRelations(Connection con, String lang) throws SQLException, IOException, ParseException {
		JSONParser parser = new JSONParser();
		FileReader reader = new FileReader("res/mapping.json");
		JSONObject obj = (JSONObject) parser.parse(reader);
		
	    String query = "select id_source, id_target, w_source, w_target, type from "+lang+"_relation";
	    System.out.println(query);
	    try (Statement stmt = con.createStatement()) {
	      ResultSet rs = stmt.executeQuery(query);
	      while (rs.next()) {
	    	  String synset_from = rs.getString("id_source");
	    	  String synset_to = rs.getString("id_target");
	    	  String new_id_from;
	    	  try{
	    		  new_id_from = (String) obj.get(synset_from);
	    	  }catch(Exception e) {
	    		  continue;
	    	  }
	    	  String new_id_to;
	    	  try{
	    		  new_id_to = (String) obj.get(synset_to);
	    	  }catch(Exception e) {
	    		  continue;
	    	  }
	    	  String lemma_from = rs.getString("w_source");
	    	  String lemma_to = rs.getString("w_target");
	    	  String rel = rs.getString("type");
		        try ( WorldGraph wg = new WorldGraph( "bolt://localhost:7687", "neo4j", "memoria" ) )
		        {
		            wg.addRelation(new_id_from, new_id_to, lemma_from, lemma_to, rel, lang);
		        }catch(Exception e){
		        	System.out.println("Errore");
		        	System.out.println(e);
		        }
	      }
	    } catch (SQLException e) {
	    	System.out.println(e);
	    }
	  }
	  public static void main( String args[] ) {
	      Connection c = null;
	      
	      try {
	         Class.forName("org.sqlite.JDBC");
	         c = DriverManager.getConnection("jdbc:sqlite:res/mwn.db");
	         
	         //addSynsets(c, "english");
	         //addRelations(c, "english");
	         //addSynsets(c, "italian");

	         //addUWN();
	         addSynsets(c, "french");
	         addSynsets(c, "portuguese");
	         addSynsets(c, "spanish");
	         addSynsets(c, "hebrew");
	         addSynsets(c, "latin");
	         addRelations(c, "hebrew");
	         addRelations(c, "latin");
	      } catch ( Exception e ) {
	         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	         System.exit(0);
	      }
	      
	   }
}
