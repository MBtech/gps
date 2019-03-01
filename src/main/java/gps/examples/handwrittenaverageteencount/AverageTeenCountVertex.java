package gps.examples.handwrittenaverageteencount;

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
 * GPS implementation of AverageTeenCount algorithm.
 * 
 * @author semihsalihoglu
 */
public class AverageTeenCountVertex extends NullEdgeVertex<IntWritable, BooleanWritable> {

	@Override
	public void compute(Iterable<BooleanWritable> incomingMessages, int superstepNo) {
		if (superstepNo == 1) {
			if (getValue().getValue() >= 10 && getValue().getValue() < 20) {
				sendMessages(getNeighborIds(), new BooleanWritable(true));
			}
		} else if (superstepNo == 2) {
			int K = ((IntOverwriteGlobalObject) getGlobalObjectsMap().getGlobalObject("K"))
				.getValue().getValue();
			if (getValue().getValue() > K) {
				int count = 0;
				for (BooleanWritable msg : incomingMessages) {
						count++;
			    }
				getGlobalObjectsMap().putOrUpdateGlobalObject("num", new IntSumGlobalObject(1));	
				getGlobalObjectsMap().putOrUpdateGlobalObject("sum", new IntSumGlobalObject(count));	
			}			
		}
	}

	@Override
	public IntWritable getInitialValue(int id) {
		return new IntWritable(getRandom().nextInt(20) + 10);
	}
	
	public static class AverageTeenCountMaster extends Master {
		
		public static int DEFAULT_MEMBERSHIP_NUM = 0;
		public static int K;
		private double averageTeenCount;
		public AverageTeenCountMaster(CommandLine line) {
			String otherOptsStr = line.getOptionValue(GPSNodeRunner.OTHER_OPTS_OPT_NAME);
			System.out.println("otherOptsStr: " + otherOptsStr);
			K = DEFAULT_MEMBERSHIP_NUM;
			if (otherOptsStr != null) {
				String[] split = otherOptsStr.split("###");
				for (int index = 0; index < split.length; ) {
					String flag = split[index++];
					String value = split[index++];
					if ("-K".equals(flag)) {
						K = Integer.parseInt(value);
					}
				}
			}
		}
		
		@Override
		public void compute(int superstepNo) {
			if (superstepNo == 2) {
				getGlobalObjectsMap().clearNonDefaultObjects();
				getGlobalObjectsMap().putGlobalObject("K", new IntOverwriteGlobalObject(K));
				getGlobalObjectsMap().putGlobalObject("num", new IntSumGlobalObject(0));
				getGlobalObjectsMap().putGlobalObject("sum", new IntSumGlobalObject(0));
			} else if (superstepNo == 3) {
				int num = ((IntSumGlobalObject) getGlobalObjectsMap().getGlobalObject("num"))
					.getValue().getValue();
				int sum = ((IntSumGlobalObject) getGlobalObjectsMap().getGlobalObject("sum"))
					.getValue().getValue();
				this.averageTeenCount = (double) sum / (double) num;
				this.continueComputation = false;
			}
		}
		
		@Override
		public void writeOutput(BufferedWriter bw) throws IOException {
			bw.write("averageTeenCount\t" + averageTeenCount + "\n");
			bw.write("K\t" + K + "\n");
			super.writeOutput(bw);
		}
	}

	/**
	 * Factory class for {@link AverageTeenCountVertex}.
	 * 
	 * @author semihsalihoglu
	 */
	public static class AverageTeenCountVertexFactory extends NullEdgeVertexFactory<IntWritable, BooleanWritable> {

		@Override
		public NullEdgeVertex<IntWritable, BooleanWritable> newInstance(CommandLine commandLine) {
			return new AverageTeenCountVertex();
		}
	}

	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return AverageTeenCountVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return AverageTeenCountVertex.class;
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
			return AverageTeenCountMaster.class;
		}
	}
}
