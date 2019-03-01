package gps.graph;

import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;

import gps.globalobjects.GlobalObject;
import gps.globalobjects.GlobalObjectsMap;
import gps.node.MachineStats;
import gps.writable.IntWritable;
import gps.writable.MinaWritable;

public class Master {
	public static MachineStats machineStatsForMaster;
	public static GlobalObjectsMap globalObjectsMap = null;
	public boolean continueComputation = true;
	private int graphSize;

	// Required for Green-Marl compiled Master classes to compile. Green-Marl generates only Master 
	// classes that have a constructor with a CommandLine argument. They need a default constructor
	// as well.
	public Master() {}

	public Master(CommandLine line) {
		//Nothing to do;
	}

	public void compute(int superstepNo) {
		if (superstepNo > 1) {
			globalObjectsMap.clearNonDefaultObjects();
			terminateIfNumActiveVerticesIsZero();
			System.out.println("Inside master continueComputation: " + continueComputation);
		} else {
			System.out.println("Master is not doing any work before the 1st superstep");
		}
	}

	/**
	 * @return true if terminating
	 */
	public boolean terminateIfNumActiveVerticesIsZero() {
		int numActiveVertices =
			((IntWritable) getGlobalObjectsMap().getGlobalObject(
				GlobalObjectsMap.NUM_ACTIVE_VERTICES).getValue()).getValue();
		System.out.println("Inside Master.compute(). numActiveVertices: " + numActiveVertices);
		this.continueComputation = numActiveVertices > 0;
		return !(numActiveVertices > 0);
	}

	public GlobalObjectsMap getGlobalObjectsMap() {
		return globalObjectsMap;
	}
	
	public int getGraphSize() {
		return graphSize;
	}
	
	public void setGraphSize(int graphSize) {
		this.graphSize = graphSize;
	}

	public MachineStats getMachineStatsForMaster() {
		return machineStatsForMaster;
	}
	
	public void writeOutput(BufferedWriter bw) throws IOException {
		writeGlobalObjectsMap(bw);
	}
	
	public void terminateComputation() {
		this.continueComputation = false;
	}
	
	public void dumpIntSumGlobalObjectIfExists(String key) {
		if (getGlobalObjectsMap().getGlobalObject(key) != null) {
			int value = ((IntWritable) getGlobalObjectsMap().getGlobalObject(key).getValue())
				.getValue();
			System.out.println(key + ": " + value);
		}
	}

	private void writeGlobalObjectsMap(BufferedWriter bw) throws IOException {
		for (String key : globalObjectsMap.keySet()) {
			GlobalObject<? extends MinaWritable> go = globalObjectsMap.getGlobalObject(key);
			bw.write(key + "\t" + go.getValue().toString() + "\n");
		}
	}
}