package gps.examples.handwritten_conductance;

import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;

import gps.globalobjects.IntOverwriteGlobalObject;
import gps.globalobjects.IntSumGlobalObject;
import gps.graph.Master;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.node.GPSJobConfiguration;
import gps.node.GPSNodeRunner;
import gps.writable.BooleanWritable;
import gps.writable.IntWritable;

/**
 * GPS implementation of Conductance algorithm.
 * 
 * @author semihsalihoglu
 */
public class ConductanceVertex extends NullEdgeVertex<IntWritable, BooleanWritable> {

	@Override
	public void compute(Iterable<BooleanWritable> incomingMessages, int superstepNo) {
		int membershipNum = ((IntOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("m"))
			.getValue().getValue();
		if (superstepNo == 1) {
			if (getValue().getValue() == membershipNum) {
				getGlobalObjectsMap().putOrUpdateGlobalObject("din", new IntSumGlobalObject(
					getNeighborIds().length));
				sendMessages(getNeighborIds(), new BooleanWritable(true));
			} else {
				getGlobalObjectsMap().putOrUpdateGlobalObject("dout", new IntSumGlobalObject(
					getNeighborIds().length));
			}
		} else if (superstepNo == 2) {
			if (getValue().getValue() != membershipNum) {
				int count = 0;
				for (BooleanWritable msg : incomingMessages) {
					count++;
				}
				getGlobalObjectsMap().putOrUpdateGlobalObject("cross", new IntSumGlobalObject(count));
			}
		}
	}

	@Override
	public IntWritable getInitialValue(int id) {
		return new IntWritable(id % 2);//getRandom().nextInt(2));
	}
	
	public static class ConductanceMaster extends Master {
		
		private int dIn;
		private int dOut;
		private float conductance;

		public static int DEFAULT_MEMBERSHIP_NUM = 0;
		public int membershipNum;
		public ConductanceMaster(CommandLine line) {
			String otherOptsStr = line.getOptionValue(GPSNodeRunner.OTHER_OPTS_OPT_NAME);
			System.out.println("otherOptsStr: " + otherOptsStr);
			membershipNum = DEFAULT_MEMBERSHIP_NUM;
			if (otherOptsStr != null) {
				String[] split = otherOptsStr.split("###");
				for (int index = 0; index < split.length; ) {
					String flag = split[index++];
					String value = split[index++];
					if ("-num".equals(flag)) {
						membershipNum = Integer.parseInt(value);
					}
				}
			}
		}

		@Override
		public void compute(int superstepNo) {
			if (superstepNo == 1) {
				getGlobalObjectsMap().putGlobalObject("din", new IntSumGlobalObject(0));
				getGlobalObjectsMap().putGlobalObject("dout", new IntSumGlobalObject(0));
				getGlobalObjectsMap().putGlobalObject("m", new IntOverwriteGlobalObject(membershipNum));
			} else if (superstepNo == 2) {
				dIn = ((IntSumGlobalObject) getGlobalObjectsMap().getGlobalObject("din")).getValue().getValue();
				dOut = ((IntSumGlobalObject) getGlobalObjectsMap().getGlobalObject("dout")).getValue().getValue();
				getGlobalObjectsMap().clearNonDefaultObjects();
				getGlobalObjectsMap().putGlobalObject("m", new IntOverwriteGlobalObject(membershipNum));
				getGlobalObjectsMap().putGlobalObject("cross", new IntSumGlobalObject(0));
			} else if (superstepNo == 3) {
				int cross = ((IntSumGlobalObject) getGlobalObjectsMap().getGlobalObject("cross")).getValue().getValue();
				float m = (dIn < dOut) ? (float) dIn : (float) dOut;
				if (m == 0) {
					conductance = (cross == 0) ? (float) 0.0 : Float.MAX_VALUE;
				} else {
					conductance = (float) cross / m;
				}
				this.continueComputation = false;
			}
		}
		
		@Override
		public void writeOutput(BufferedWriter bw) throws IOException {
			bw.write("conductance\t" + conductance + "\n");
			bw.write("dIn\t" + dIn + "\n");
			bw.write("dOut\t" + dOut + "\n");
			super.writeOutput(bw);
		}
	}

	/**
	 * Factory class for {@link ConductanceVertex}.
	 * 
	 * @author semihsalihoglu
	 */
	public static class ConductanceVertexFactory extends NullEdgeVertexFactory<IntWritable, BooleanWritable> {

		@Override
		public NullEdgeVertex<IntWritable, BooleanWritable> newInstance(CommandLine commandLine) {
			return new ConductanceVertex();
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return ConductanceVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return ConductanceVertex.class;
		}

		@Override
		public Class<?> getVertexValueClass() {
			return IntWritable.class;
		}

		@Override
		public Class<?> getMessageValueClass() {
			return BooleanWritable.class;
		}
		
		@Override
		public Class<?> getMasterClass() {
			return ConductanceMaster.class;
		}
	}
}
