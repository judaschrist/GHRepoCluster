package cn.pku.sei.GHRC.graphdb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
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
	private static final String WHOLE_GH_DB = "CompleteGHRepos";
	private static final String MSR14_GH_DB = "MSR14ReposCosineSim";
	
    private static final String DB_PATH = "D:/Documents/neo4j/" + MSR14_GH_DB;
    String greeting;
    private GraphDatabaseService graphDb = null;
    Map<Long, GHRepository> reposMap = new HashMap<Long, GHRepository>();
    String inString;

	public static void main(String[] args) {
		GHGraphBuilder hello = new GHGraphBuilder();
		hello.createDb();
		hello.shutDown();
	}
    
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
    
	public void listAllRecResults() {
		File file = new File("data/list_full" + ".csv");
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
//			writer.write("Project,1st,2nd,3rd,4th,5th");
//			writer.newLine();
			Iterable<Node> iterable = GlobalGraphOperations.at(graphDb).getAllNodes();
			for (Node node : iterable) {
				GHRepository repository = new GHRepository(node);
				System.out.println(repository);
				GHRelType[] types = GHRelType.values();
				for (GHRelType ghRelType : types) {
					writer.write(repository.getName() + "," + ghRelType.toString().toUpperCase().charAt(0));
			        List<Relationship> relatedRepos = getMostRelatedRepos(repository, ghRelType);
			        int l = relatedRepos.size();
//			        System.out.println(l);
			        for (int i = 0; i < l; i++) {
			        	Relationship rel = relatedRepos.get(l - i - 1);
						writer.write("," + rel.getProperty(GHRepository.NUM));
					}
			        writer.newLine();
				}
			}			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}   

	public void listRecResults(GHRelType type) {
		File file = new File(type + ".csv");
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(file));	        	
			Iterable<Node> iterable = GlobalGraphOperations.at(graphDb).getAllNodes();
			for (Node node : iterable) {
				GHRepository repository = new GHRepository(node);
				System.out.println(repository);
				writer.write(repository.getName());
		        List<Relationship> relatedRepos = getMostRelatedRepos(repository, type);
		        int l = relatedRepos.size();
		        for (int i = 0; i < 5 && i < l; i++) {
					writer.write("," + relatedRepos.get(l - i - 1).getOtherNode(node).getProperty(GHRepository.NAME));
				}
		        writer.newLine();
			}
			writer.close();			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}   
    
    void createDb()
    {
        
        // START SNIPPET: transaction
//    	addWatchedBySameRel();
//    	addForkedBySameRel();
//    	addBySameRels();
//    	addBySameDist();

//    	generateRepoNodes();
        try (Transaction tx = graphDb.beginTx())
        {
//        	Iterator<Node> iterator = GlobalGraphOperations.at(graphDb).getAllNodes().iterator();

//        	System.out.println(inString);
        	
//            Iterator<GHRepository> repoIt = reposMap.values().iterator();
//            System.out.println(IteratorUtil.count(repoIt));
        	
//            while (repoIt.hasNext()) {
//				GHRepository ghRepository = (GHRepository) repoIt.next();
//				System.out.println(ghRepository.toString());
//			}
            
        	listAllRecResults();
//        	listRecResults(GHRelType.ISSUE_COMMENTED_BY_SAME);
        	
//            long ghid = 74915;
//            System.out.println();
//            System.out.println();
//            List<GHRepository> relatedRepos = getMostRelatedRepos(reposMap.get(ghid));
//            for (GHRepository ghRepository : relatedRepos) {
//				System.out.println(ghRepository);
//			}
            
            
//        	Iterator<Relationship> relIt = GlobalGraphOperations.at(graphDb).getAllRelationships().iterator();
//			while (relIt.hasNext()) {
//				Relationship rel = relIt.next();
//				if (rel.isType(GHRelType.ISSUE_COMMENTED_BY_SAME)) {
//					rel.delete();
//					System.out.println("del!");
//				}
//				
//			}
//			repos.get(0).createRelTo( repos.get(1), GHRelType.WATCHED_BY_SAME ).setProperty(GHRepository.NUM, 1);
//			repos.get(2).createRelTo( repos.get(3), GHRelType.WATCHED_BY_SAME ).setProperty(GHRepository.NUM, 1);
//			repos.get(4).createRelTo( repos.get(5), GHRelType.WATCHED_BY_SAME ).setProperty(GHRepository.NUM, 1);
//			repos.get(5).createRelTo( repos.get(6), GHRelType.WATCHED_BY_SAME ).setProperty(GHRepository.NUM, 1);
//			repos.get(6).createRelTo( repos.get(4), GHRelType.WATCHED_BY_SAME ).setProperty(GHRepository.NUM, 1);
//			repos.get(1).createRelTo( repos.get(2), GHRelType.WATCHED_BY_SAME ).setProperty(GHRepository.NUM, 1);          
           
            tx.success();
        }
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
				ArrayRealVector v1 = fetcher.getMemberVector(ghid1);
				System.out.println("-----------------" + repo1 + "--------------------");
				for (int j = i+1; j < n; j++) {
					repo2 = new GHRepository(graphDb.getNodeById(j));
					System.out.println(repo2);
					long ghid2 = Long.parseLong(repo2.getGHid());
					ArrayRealVector v2 = fetcher.getMemberVector(ghid2);
					try {
						sim = v1.cosine(v2);
					} catch (MathArithmeticException e) {
						sim = 0;
					}
					if (sim > 0) {
						repo1.createRelTo(repo2, GHRelType.MEMBER_BY_SAME).setProperty(GHRepository.NUM, sim);
					}
					System.out.println(sim);
				}
				tx.success();
			}
		}
		fetcher.close();
	}
    
	public List<Relationship> getMostRelatedRepos(GHRepository ghRepository, GHRelType type) {
		Node node = ghRepository.getNode();
		List<Relationship> rels = new ArrayList<>();
		System.out.println(ghRepository.getNode().getDegree(type));
		System.out.println(type.toString() + IteratorUtil.count(node.getRelationships(type)));
		IteratorUtil.addToCollection(node.getRelationships(type), rels);
		Collections.sort(rels, (rel1, rel2) -> Double.compare(GHRepository.getScore(rel1), GHRepository.getScore(rel2)));
		return rels;
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
		ResultSet rs = fetcher.getProjectColumns();
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
		String value = null;
		int count;
		int total = 0;
		int step = 5000;
		while (true) {
			System.out.println(total);
			count = 0;
			try (Transaction tx = graphDb.beginTx()){
				while (count < step) {
					if (!rs.next()) {
						break;
					}
					Node node = graphDb.createNode();
					for (int i = 1; i <= propCount; i++) {
						value = rs.getString(i);
						value = value == null ? "":value;
						node.setProperty(colNames[i], value);
					}
					count++;
					total++;
				}
				
				tx.success();
			}
			if (count < step) {
				break;
			}
		}
		
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
