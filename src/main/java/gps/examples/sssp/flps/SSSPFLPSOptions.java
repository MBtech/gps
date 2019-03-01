package gps.examples.sssp.flps;

import gps.node.GPSNodeRunner;

import org.apache.commons.cli.CommandLine;

public class SSSPFLPSOptions {
	public static String GOBJ_COMP_PHASE = "p";
	public static String GOBJ_NUM_POTENTIALLY_ACTIVE_VERTICES = "av";
	public static String GOBJ_NUM_RECENTLY_UPDATED_VERTICES = "uv";
	
	public static String NUM_EDGES_THRESHOLD_FOR_SERIAL_LABEL_PROPAGATION = "netfslp";

	public int numEdgesThresholdForFLPS = 1000000;
	
	protected void parseOtherOpts(CommandLine commandLine) {
		String otherOptsStr = commandLine.getOptionValue(GPSNodeRunner.OTHER_OPTS_OPT_NAME);
		System.out.println("otherOptsStr: " + otherOptsStr);
		if (otherOptsStr != null) {
			String[] split = otherOptsStr.split("###");
			for (int index = 0; index < split.length; ) {
				String flag = split[index++];
				String value = split[index++];
				if (NUM_EDGES_THRESHOLD_FOR_SERIAL_LABEL_PROPAGATION.equals(flag)) {
					numEdgesThresholdForFLPS = Integer.parseInt(value);
				}
			}
		}
	}
}
