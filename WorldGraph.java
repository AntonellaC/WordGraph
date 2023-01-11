package wordgraph;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;


import static org.neo4j.driver.Values.parameters;

public class WorldGraph implements AutoCloseable
{
    private final Driver driver;

    public WorldGraph( String uri, String user, String password )
    {
        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
    }

    @Override
    public void close() throws Exception
    {
        driver.close();
    }
    public Boolean addUWN(String synset, String rel, String other_lang, Boolean from) {
    	System.out.println("AddUWN "+synset+" "+rel +" "+other_lang+" "+from);
    	try ( Session session = driver.session() )
        {
    	Boolean res = (Boolean) session.writeTransaction( tx ->
            {
           Result result = tx.run( "MATCH (n:Category {id:$idx}) " +
        		"WITH COUNT(n) as esiste " +
           		"RETURN esiste",
           		parameters( "idx", synset ) );
           return result.single().get( 0 ).asInt()>0;
                                                   } );
    	System.out.println("Exists: "+ res);
    	if (!res) return false;
    	res = (Boolean) session.writeTransaction( tx ->
        {
       Result result = tx.run( "MATCH (n:UWNCategory {id:$other_lang}) " +
    		"WITH COUNT(n) as esiste " +
       		"RETURN esiste",
       		parameters( "other_lang", other_lang ) );
       return result.single().get( 0 ).asInt()>0;                                         } );
    	if(!res) {
    		session.writeTransaction( tx ->
            {
           Result result = tx.run( "CREATE (n:UWNCategory {id:$other_lang}) " +
           		"RETURN n.id",
           		parameters( "other_lang", other_lang ) );
           return result.single().get( 0 ).asString();                                         } );
    	}
    	
    	if (from) {
    		String out = session.writeTransaction( tx ->
                 {
                Result result = tx.run( "MATCH (n:Category), " +
                		" (uwn:UWNCategory) " +
                		"WHERE n.id=$idx " +
                		"AND uwn.id=$other_lang " +
                		"CREATE (n) -[r:UWNRel {type:$rel}]-> (uwn)",
                		parameters( "idx",synset , "rel", rel, "other_lang", other_lang) );
                return result.single().get( 0 ).asString();
                                                        } );
            System.out.println( out );
    	}else {
    		String out = session.writeTransaction( tx ->
            {
           Result result = tx.run( "MATCH (n:Category), " +
           		" (uwn:UWNCategory) " +
           		"WHERE n.id=$idx " +
           		"AND uwn.id=$other_lang " +
           		"CREATE (uwn) -[r:UWNRel {type:$rel}]-> (n) "+
           		"RETURN n.id",
           		parameters( "idx",synset , "rel", rel, "other_lang", other_lang) );
           return result.single().get( 0 ).asString();
                                                   } );
       System.out.println( out );
    	}
        }catch(Exception e){
        	System.out.println(e);
        	return false;
        }
            
    	return true;
    }
    
    public Boolean addSynset(String id, String word, String phrase, String gloss, String lang, double negScore, double objScore, double posScore) {
    	try ( Session session = driver.session() )
        {
    	/*Boolean res = session.writeTransaction( tx ->
            {
           Result result = tx.run( "MATCH (n:Category {id:$idx, name:$word, description:$gloss, language:$lang, taxonomy:$taxonomy}) " +
        		"WITH COUNT(n) as esiste " +
           		"RETURN esiste",
           		parameters( "idx", id, "word", word, "taxonomy", "WordNet", "gloss", gloss, "lang", lang ) );
           return result.single().get( 0 ).asInt()>0;
                                                   } );*/
    		Boolean res = false;
    		                   
    	if (res) return false;
    	if (negScore<0) {
    		
    		String out = session.writeTransaction( tx ->
                 {
                Result result = tx.run( "CREATE (n:Category {id:$idx, name:$word, description:$gloss, language:$lang, taxonomy:$taxonomy}) " +
                		"RETURN 'creato nodo #' + id(n) + ': ' + $idx",
                		parameters( "idx", id, "word", word, "taxonomy", "WordNet", "gloss", gloss, "lang", lang ) );
                return result.single().get( 0 ).asString();
                                                        } );
            System.out.println( out );
            }else {
            	String out = session.writeTransaction( tx ->
                {
               Result result = tx.run( "CREATE (n:Category {id:$idx, name:$word, description:$gloss, language:$lang, taxonomy:$taxonomy, negScore:$neg, objScore:$obj, posScore:$pos}) " +
               		"RETURN 'creato nodo #' + id(n) + ': ' + $idx",
               		parameters( "idx", id, "word", word, "taxonomy", "WordNet", "gloss", gloss, "lang", lang, "neg", negScore, "obj", objScore, "pos", posScore) );
               return result.single().get( 0 ).asString();
                                                       } );
            	System.out.println( out );
            }
        }catch(Exception e) {
        	System.out.println("add Synset");
        	System.out.println(e);
        }
    	return true;
    }
    
    public static String convertPos(String pos) throws Exception {
    	switch(pos) {
    	case "n": 
    		return "Noun";
    	case "v": 
    		return "Verb";
    	case "a": 
    		return "Adjective";
    	case "s": 
    		return "Adjective satellite";
    	case "r": 
        	return "Adverb";
    	default:
    		throw new Exception("Invalid pos: "+pos); 
    	}
    }

    public void addLemma(String synset_id, String pos, String lemma, String is_phrase, String lang )
    {
    	

    	 		try ( Session session = driver.session() )
            {
                String out = session.writeTransaction( tx ->
                                                            {
                                                                Result result = null;
																try {
																	result = tx.run( "CREATE (n:Word {synset_id:$synset_id, pos:$pos, lemma:$lemma, language:$lang}) " +
																	                        "RETURN 'creato nodo #' + id(n) + ': ' + $lemma",
																	                        parameters( "synset_id", synset_id, "pos", convertPos(pos), "lemma", lemma, "lang", lang ) );
																} catch (Exception e) {
																	// TODO Auto-generated catch block
																	e.printStackTrace();
																}
                                                                return result.single().get( 0 ).asString();
                                                            } );
                System.out.println( "Creato lemma: "+out+ ", " +synset_id);
            
                out = session.writeTransaction( tx ->
                                                            {
                                                                Result result = tx.run( "MATCH\r\n"
                                                                		+ "  (word:Word),\r\n"
                                                                		+ "  (category:Category)\r\n"
                                                                		+ "WHERE word.synset_id = $synset_id AND category.id = $synset_id"
                                                                		+ " AND word.language = $lang AND category.language = $lang AND word.lemma=$lemma\r\n"
                                                                		+ "CREATE (word)-[r:Word_expresses_Category]->(category)\r\n"
                                                                		+ "RETURN 'creata relazione Word_expresses_Category' + $synset_id + ' ' + $lemma",
                                                                          parameters( "synset_id", synset_id, "lang", lang, "lemma",lemma) );
                                                                return result.single().get( 0 ).asString();
                                                            } );
                System.out.println( out );
            }
    		

       
    }
    
    public static String getRelation(String rel) {
    	switch(rel) {
    	case "!": 
    		return "Word_antonymousOf_Word";
    	case "@": 
    		return "Word_isA_Word";
    	case "~": 
    		return "Word_instanceOf_Word";
    	case "#m": 
    		return "Word_partOf_Word {type:Membership}";
    	case "#s": 
    		return "Word_partOf_Word {type:Substance}";
    	case "#p": 
    		return "Word_partOf_Word {type:Part}";
    	case "%m": 
    		return "Word_hasPart_Word {type:Membership}";
    	case "%s": 
    		return "Word_hasPart_Word {type:Substance}";
    	case "%p": 
    		return "Word_hasPart_Word {type:Part}";
    	case "-c": 
    		return "Word_derivesFrom_Word";
    	case ">": 
    		return "Word_causes_Word";
    	case "=": 
    		return "Word_attributeOf_Word";
    	case "^": 
    		return "Word_seeAlso_Word";
    	case "*": 
    		return "Word_entails_Word";
    	case "<": 
    		return "Word_pastParticipleOf_Word";
    	case "|": 
    		return "Word_nearestOf_Word";
    	case "/": 
    		return "Word_pertainsTo_Word";
    	case "-r": 
    		return "Word_isRoleOf_Word";
    	case "+r": 
    		return "Word_hasRole_Word";
    	case "$": 
    		return "Word_symilarMeaningAs_Word";
    	default:
    		return null;
    	}//|, +r, -r, /, $, 
    	
    }
    
    public void addRelation(String synset_id_from, String synset_id_to, String lemma_from, String lemma_to, String rel_symbol, String lang )
    {
    	System.out.println(rel_symbol);
    	final String rel = getRelation(rel_symbol);
    	if (rel == null) return;
    	 		try ( Session session = driver.session() )
            {
            
                String out = session.writeTransaction( tx ->
                                                            {
                                                                Result result = tx.run( "MATCH\r\n"
                                                                		+ "  (word_from:Word),\r\n"
                                                                		+ "  (word_to:Word)\r\n"
                                                                		+ "WHERE word_from.synset_id = $synset_id_from AND word_to.synset_id = $synset_id_to"
                                                                		+ " AND word_from.language = $lang AND word_to.language = $lang AND word_from.lemma=$lemma_from AND word_to.lemma=$lemma_to\r\n"
                                                                		+ "CREATE (word_from)-[r:"+rel+"]->(word_to)\r\n"
                                                                		+ "RETURN 'creata relazione $rel' + $synset_id_from + ' ' +  $synset_id_from",
                                                                          parameters( "synset_id_from", synset_id_from, "lang", lang, "synset_id_to",synset_id_to, "lemma_from", lemma_from, "lemma_to", lemma_to, "rel", rel) );
                                                                return result.single().get( 0 ).asString();
                                                            } );
                System.out.println( out );
            }
    		

       
    }

}