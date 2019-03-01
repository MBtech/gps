package gps.examples.scc.singlepivot;

import org.apache.commons.cli.CommandLine;

import gps.examples.scc.SCCBaseMaster;
import gps.examples.scc.SCCComputationPhase.Phase;

public class SinglePivotMaster extends SCCBaseMaster {

	public SinglePivotMaster(CommandLine commandLine) {
		super(commandLine);
	}

	@Override
	public Phase getNextInitialComputationStage(int superstepNo) {
		return Phase.ROOT_PICKING;
	}
}