package gps.examples.wcc.hybrid;

import gps.node.GPSNodeRunner;

import org.apache.commons.cli.CommandLine;

public class SinglePivotHybridWCCOptions {

	public static final String GOBJ_COMP_PHASE = "phase";
	public static final String GOBJ_RANDOM_ROOT_PICKER = "rrp";
	public static final String GOBJ_NUM_PROPAGATING_VERTICES = "npv"; 
		
	public static final String NUM_EDGES_FOR_SERIAL_COMPUTATION = "nefsc";
	public int numEdgesForSerialComputation = 1000000;

	protected void parseOtherOpts(CommandLine commandLine) {
		String otherOptsStr = commandLine.getOptionValue(GPSNodeRunner.OTHER_OPTS_OPT_NAME);
		System.out.println("otherOptsStr: " + otherOptsStr);
		if (otherOptsStr != null) {
			String[] split = otherOptsStr.split("###");
			for (int index = 0; index < split.length; ) {
				String flag = split[index++];
				String value = split[index++];
				if (NUM_EDGES_FOR_SERIAL_COMPUTATION.equals(flag)) {
					numEdgesForSerialComputation = Integer.parseInt(value);
				}
			}
		}
	}
}
