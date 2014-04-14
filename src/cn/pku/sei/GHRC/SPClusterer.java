package cn.pku.sei.GHRC;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.Transaction;

import cn.pku.sei.GHRC.graphdb.GHGraphBuilder;

import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.datagenerators.ClusterDefinition;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Remove;

public class SPClusterer {
	public static void main(String[] args) throws Exception {
//		ArffLoader loader = new ArffLoader();
//		loader.setFile(new File("data/test.arff"));
//		Instances data = loader.getDataSet();
//		String[] options = new String[2];
//		options[0] = "-R"; // "range"
//		options[1] = "1"; // first attribute
//		Remove remove = new Remove(); // new instance of filter
//		remove.setOptions(options); // set options
//		remove.setInputFormat(data); // inform filter about dataset		
//		Instances filteredData = Filter.useFilter(data, remove);
//		System.out.println(filteredData.toString());	
		GHGraphBuilder builder = new GHGraphBuilder();
		MySpectralClusterer spClusterer = new MySpectralClusterer();

		spClusterer.setAlphaStar(0.6);
		spClusterer.buildClusterer(builder);
		System.out.println("--------------result-------------");
		try (Transaction tx = builder.getGraphDb().beginTx()){
			for (int i = 0; i < spClusterer.numberOfClusters(); i++) {
				System.out.println("Cluster " + i);
				for (int j = 0; j < spClusterer.cluster.length; j++) {
					if (spClusterer.cluster[j] == i) {
						System.out.println(builder.getGHRepoNodeById(j).toString());
					}
				}
			}
			
			tx.success();
		}
		builder.shutDown();
	}
}
