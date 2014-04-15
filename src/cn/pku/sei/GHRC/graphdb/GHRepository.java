package cn.pku.sei.GHRC.graphdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import cn.pku.sei.GHRC.graphdb.GHRepository.GHRelType;

public class GHRepository {
	
	public static final String GHID = "gh-id";

	public static enum GHRelType implements RelationshipType
    {
        WATCHED_BY_SAME,
    	CONTRIBUTE_BY_SAME,
    }
	
	public static final String NAME = "name";
	public static final String DESCRIPTION = "description";
	public static final String NUM = "num";
	private final Node node;
	
	GHRepository (Node node) {
		this.node = node;
	}
	
	public Node getNode() {
		return node;
	}
	
	public String getName() {
        return (String)node.getProperty(NAME);
    }
	
	public String getDescription() {
        return (String)node.getProperty(DESCRIPTION);
    }
	
	@Override
    public int hashCode() {
        return node.hashCode();
    }

    @Override
    public boolean equals( Object o )
    {
        return o instanceof GHRepository &&
                node.equals( ( (GHRepository)o ).getNode() );
    }

    @Override
    public String toString()
    {
    	Iterator<String> it = node.getPropertyKeys().iterator();
    	String s = node.getProperty(NAME)+":\t"+node.getProperty(DESCRIPTION);
//    	while (it.hasNext()) {
//			String key = it.next();
//			s += key + ":" + node.getProperty(key) + ", ";
//		}
        return s + " REPO:" + node.getId() + " GH:" + getGHid();
    }

	public void setName(String string) {
		node.setProperty(NAME, string);
		
	}

	public void setDescription(String string) {
		node.setProperty(DESCRIPTION, string);		
	}

	public long getId() {
		return node.getId();
	}

	public Iterator<Relationship> getRelationships() {		
		return node.getRelationships(Direction.OUTGOING).iterator();
	}

	public static int getScore(Relationship rel) {
		int num = (Integer)rel.getProperty(NUM);
		int multiplier = 0;
		if (rel.isType(GHRelType.WATCHED_BY_SAME)) {
			multiplier = 1;
		} else if (rel.isType(GHRelType.CONTRIBUTE_BY_SAME)) {
			multiplier = 1;
		}
		return num * multiplier;
	}

	public String getGHid() {
        return (String)node.getProperty(GHID);
	}

	
	public Relationship createRelTo(GHRepository anotherRepo, GHRelType type) {
		return node.createRelationshipTo(anotherRepo.getNode(), type);
	}
	
	public void addRelNum(GHRepository rep2, GHRelType type) {
		Iterator<Relationship> it = node.getRelationships(type).iterator();
		if (it.hasNext()) {
			Relationship rel = it.next();
			if (rel.getOtherNode(node).getId() == rep2.getNode().getId()) {
				rel.setProperty(NUM, (Integer)rel.getProperty(NUM) + 1);
				return;
			}
		} 
		createRelTo(rep2, type).setProperty(NUM, 1);
	}


}
