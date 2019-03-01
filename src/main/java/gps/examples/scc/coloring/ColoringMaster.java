package gps.examples.scc.coloring;

import org.apache.commons.cli.CommandLine;

import gps.examples.scc.SCCBaseMaster;
import gps.examples.scc.SCCComputationPhase.Phase;

/**
 * Master class for the Coloring algorithm from Orzan's thesis:
 * (https://www.cs.vu.nl/en/Images/SM%20Orzan%205-11-2004_tcm75-258582.pdf)
 *
 * @author semihsalihoglu
 */
public class ColoringMaster extends SCCBaseMaster {

	public ColoringMaster(CommandLine commandLine) {
		super(commandLine);	}

	@Override
	public Phase getNextInitialComputationStage(int superstepNo) {
		return Phase.FW_1;
	}
}