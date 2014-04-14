package cn.pku.sei.GHRC;

import java.util.Enumeration;

import weka.core.DistanceFunction;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.neighboursearch.PerformanceStats;

public class RepoDistance implements DistanceFunction{

	@Override
	public Enumeration listOptions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setOptions(String[] options) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String[] getOptions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setInstances(Instances insts) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Instances getInstances() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setAttributeIndices(String value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getAttributeIndices() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setInvertSelection(boolean value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean getInvertSelection() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public double distance(Instance first, Instance second) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double distance(Instance first, Instance second,
			PerformanceStats stats) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double distance(Instance first, Instance second, double cutOffValue) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double distance(Instance first, Instance second, double cutOffValue,
			PerformanceStats stats) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void postProcessDistances(double[] distances) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void update(Instance ins) {
		// TODO Auto-generated method stub
		
	}

}
