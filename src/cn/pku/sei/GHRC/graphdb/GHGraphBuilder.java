package cn.pku.sei.GHRC.graphdb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.exception.MathArithmeticException;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.tooling.GlobalGraphOperations;

import cn.pku.sei.GHRC.graphdb.GHRepository.GHRelType;

public class GHGraphBuilder {
    private static final String DB_PATH = "D:/Documents/neo4j/MSR14ReposCosineSim";
    String greeting;
    // START SNIPPET: vars
    private GraphDatabaseService graphDb = null;
    Map<Long, GHRepository> reposMap = new HashMap<Long, GHRepository>();
    String inString;

    public GHGraphBuilder() {
    	if (graphDb == null) {
        	graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        	registerShutdownHook(graphDb);
		}
    	try (Transaction tx = graphDb.beginTx())
        {
	    	Iterator<Node> nodesIt = GlobalGraphOperations.at(graphDb).getAllNodes().iterator();
	        while (nodesIt.hasNext()) {
				GHRepository tempRepository = new GHRepository(nodesIt.next());
				reposMap.put(Long.parseLong(tempRepository.getGHid()), tempRepository);
			}
			
			Iterator<GHRepository> repoIt = reposMap.values().iterator();
			inString = "(" + repoIt.next().getGHid();
            while (repoIt.hasNext()) {
				GHRepository ghRepository = repoIt.next();
				inString += "," + ghRepository.getGHid();
			}
			inString += ")";
	        tx.success();
        }
	}

    public GraphDatabaseService getGraphDb() {
    	return graphDb;
    }
    
    void createDb()
    {
        
        
        
        // START SNIPPET: transaction
//    	addWatchedBySameRel();
//    	addForkedBySameRel();
//    	addBySameRels();
//    	addBySameDist();
        try (Transaction tx = graphDb.beginTx())
        {

//        	generateRepoNodes();
//        	System.out.println(inString);
        	
            Iterator<GHRepository> repoIt = reposMap.values().iterator();
        	
//            while (repoIt.hasNext()) {
//				GHRepository ghRepository = (GHRepository) repoIt.next();
//				System.out.println(ghRepository.toString());
//			}
            
//            long ghid = 76946;
//            System.out.println();
//            System.out.println();
//            List<GHRepository> relatedRepos = getMostRelatedRepos(reposMap.get(ghid));
//            for (GHRepository ghRepository : relatedRepos) {
//				System.out.println(ghRepository);
//			}
            
            
          Iterator<Relationship> relIt = GlobalGraphOperations.at(graphDb).getAllRelationships().iterator();
			while (relIt.hasNext()) {
				Relationship rel = relIt.next();
//				rel.delete();
			}
//			repos.get(0).createRelTo( repos.get(1), GHRelType.WATCHED_BY_SAME ).setProperty(GHRepository.NUM, 1);
//			repos.get(2).createRelTo( repos.get(3), GHRelType.WATCHED_BY_SAME ).setProperty(GHRepository.NUM, 1);
//			repos.get(4).createRelTo( repos.get(5), GHRelType.WATCHED_BY_SAME ).setProperty(GHRepository.NUM, 1);
//			repos.get(5).createRelTo( repos.get(6), GHRelType.WATCHED_BY_SAME ).setProperty(GHRepository.NUM, 1);
//			repos.get(6).createRelTo( repos.get(4), GHRelType.WATCHED_BY_SAME ).setProperty(GHRepository.NUM, 1);
//			repos.get(1).createRelTo( repos.get(2), GHRelType.WATCHED_BY_SAME ).setProperty(GHRepository.NUM, 1);          
           
            tx.success();
        }
    }

    class relComparator implements Comparator<Relationship> {
		@Override
		public int compare(Relationship o1, Relationship o2) {
			return (int) (GHRepository.getScore(o1) - GHRepository.getScore(o2));
		}    	
    }
    
	public List<GHRepository> getMostRelatedRepos(GHRepository ghRepository) {
		Node node = ghRepository.getNode();
		List<GHRepository> repos = new ArrayList<GHRepository>();
		List<Relationship> rels = new ArrayList<>();
		IteratorUtil.addToCollection(node.getRelationships(), rels);
		Collections.sort(rels, new relComparator());
		for (Relationship relationship : rels) {
			repos.add(new GHRepository(relationship.getOtherNode(node)));
		}
		return repos;
	}

	private void printToCSV(GHRepository ghRepository) {
		File file = new File("repos.csv");
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
			writer.write(ghRepository.getNode().getId() + "," + 
					ghRepository.getGHid() + ",\"" + 
					ghRepository.getName() + "\",\"" + 
					ghRepository.getDescription() + "\",\"" + 
					(String)ghRepository.getNode().getProperty("url") + "\"");
			writer.newLine();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
	}

	private void addWatchedBySameRel() {
		MySQLFetcher fetcher = new MySQLFetcher();
		
		int n = reposMap.size();
		GHRepository repo1;
		GHRepository repo2;
		
		for (int i = 0; i < n; i++) {
			try (Transaction tx = graphDb.beginTx()) {
				repo1 = new GHRepository(graphDb.getNodeById(i));
				System.out.println("-----------------" + repo1 + "--------------------");
				for (int j = i+1; j < n; j++) {
					repo2 = new GHRepository(graphDb.getNodeById(j));
					int sum = fetcher.countWatchedBySameNum(Long.parseLong(repo1.getGHid()), Long.parseLong(repo2.getGHid()));
					if (sum > 0) {
						repo1.createRelTo(repo2, GHRelType.WATCHED_BY_SAME).setProperty(GHRepository.NUM, sum);
					}
					System.out.println(sum);
				}
				tx.success();
			}
		}
		fetcher.close();
	}
	
	private void addBySameDist() {
		MySQLFetcher fetcher = new MySQLFetcher();
		
		int n = reposMap.size();
		GHRepository repo1;
		GHRepository repo2;
		double dist = 0;
		
		for (int i = 0; i < n; i++) {
			try (Transaction tx = graphDb.beginTx()) {
				repo1 = new GHRepository(graphDb.getNodeById(i));
				long ghid1 = Long.parseLong(repo1.getGHid());
				ArrayRealVector v1 = fetcher.getWatchVector(ghid1);
				System.out.println("-----------------" + repo1 + "--------------------");
				for (int j = i+1; j < n; j++) {
					repo2 = new GHRepository(graphDb.getNodeById(j));
					System.out.println(repo2);
					long ghid2 = Long.parseLong(repo2.getGHid());
					ArrayRealVector v2 = fetcher.getWatchVector(ghid2);
					dist = v1.getDistance(v2); 
					repo1.createRelTo(repo2, GHRelType.WATCHED_BY_SAME).setProperty(GHRepository.NUM, dist);
					System.out.println(dist);
				}
				tx.success();
			}
		}
		fetcher.close();
	}
	
	private void addBySameRels() {
		MySQLFetcher fetcher = new MySQLFetcher();
		
		int n = reposMap.size();
		GHRepository repo1;
		GHRepository repo2;
		double sim = 0;
		
		for (int i = 0; i < n; i++) {
			try (Transaction tx = graphDb.beginTx()) {
				repo1 = new GHRepository(graphDb.getNodeById(i));
				long ghid1 = Long.parseLong(repo1.getGHid());
				ArrayRealVector v1 = fetcher.getForkVector(ghid1);
				System.out.println("-----------------" + repo1 + "--------------------");
				for (int j = i+1; j < n; j++) {
					repo2 = new GHRepository(graphDb.getNodeById(j));
					System.out.println(repo2);
					long ghid2 = Long.parseLong(repo2.getGHid());
					ArrayRealVector v2 = fetcher.getForkVector(ghid2);
					try {
						sim = v1.cosine(v2);
					} catch (MathArithmeticException e) {
						sim = 0;
					}
					if (sim > 0) {
						repo1.createRelTo(repo2, GHRelType.FORKED_BY_SAME).setProperty(GHRepository.NUM, sim);
					}
					System.out.println(sim);
				}
				tx.success();
			}
		}
		fetcher.close();
	}

	private void addForkedBySameRel() {
		MySQLFetcher fetcher = new MySQLFetcher();
		
		int n = reposMap.size();
		GHRepository repo1;
		GHRepository repo2;
		
		for (int i = 0; i < n; i++) {
			try (Transaction tx = graphDb.beginTx()) {
				repo1 = new GHRepository(graphDb.getNodeById(i));
				System.out.println("-----------------" + repo1 + "--------------------");
				for (int j = i+1; j < n; j++) {
					repo2 = new GHRepository(graphDb.getNodeById(j));
					int sum = fetcher.countForkedBySameNum(Long.parseLong(repo1.getGHid()), Long.parseLong(repo2.getGHid()));
					if (sum > 0) {
						repo1.createRelTo(repo2, GHRelType.FORKED_BY_SAME).setProperty(GHRepository.NUM, sum);
					}
					System.out.println(sum);
				}
				tx.success();
			}
		}
		fetcher.close();
	}
	
	private void generateRepoNodes() {
		MySQLFetcher fetcher = new MySQLFetcher();
		ResultSet rs = fetcher.getColumn("projects", "id, url, name, description", "forked_from IS null");
		try {
			createNodesFromSqlResult(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		fetcher.close();
	}
    
	private void createNodesFromSqlResult(ResultSet rs) throws SQLException {
		int propCount = rs.getMetaData().getColumnCount();
		String[] colNames = new String[propCount + 1];
		for (int i = 1; i <= propCount; i++) {
			String temp = rs.getMetaData().getColumnName(i);
			colNames[i] = temp.equals("id") ? "gh-" + temp : temp;
		}
		while (rs.next()) {
			Node node = graphDb.createNode();
			for (int i = 1; i <= propCount; i++) {
				node.setProperty(colNames[i], rs.getString(i));
			}
		}		
	}

	public static void main(String[] args) {
		GHGraphBuilder hello = new GHGraphBuilder();
        hello.createDb();
        hello.shutDown();
	}
	
	public void shutDown()
    {
        System.out.println();
        System.out.println( "Shutting down database ..." );
        // START SNIPPET: shutdownServer
        graphDb.shutdown();
        // END SNIPPET: shutdownServer
    }
	
	private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }

	public int getNodeNum() {
		int n = 0;
		try (Transaction tx = graphDb.beginTx()) {
			n = IteratorUtil.count(GlobalGraphOperations.at(graphDb).getAllNodes());
			tx.success();
		}
		return n;
	}

	public List<GHRepository> getAllRepos() {
		List<GHRepository> repos = new ArrayList<GHRepository>();
    	Iterator<Node> nodesIt = GlobalGraphOperations.at(graphDb).getAllNodes().iterator();
        while (nodesIt.hasNext()) {
			GHRepository tempRepository = new GHRepository(nodesIt.next());
			repos.add(tempRepository);
		}
        return repos;
	}

	public Iterator<Relationship> getAllRelationships() {
		return GlobalGraphOperations.at(graphDb).getAllRelationships().iterator();
	}

	public GHRepository getGHRepoNodeById(int j) {
		return new GHRepository(graphDb.getNodeById(j));
	}
}
