package gps.examples.randomwalk;

import java.util.Random;

import org.apache.commons.cli.CommandLine;

import gps.examples.sssp.SingleSourceAllVerticesShortestPathVertex;
import gps.examples.sssp.SingleSourceAllVerticesShortestPathVertex.SingleSourceAllVerticesShortestPathVertexFactory;
import gps.globalobjects.IntSumGlobalObject;
import gps.globalobjects.LongSumGlobalObject;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.graph.VertexFactory;
import gps.node.GPSJobConfiguration;
import gps.node.GPSNodeRunner;
import gps.writable.IntWritable;

public class RandomWalkVertex extends NullEdgeVertex<IntWritable, IntWritable> {

	public static int[] messagesToNeighbors = new int[2];
	private static int initialNumWalkers;
	private static final int DEFAULT_NUM_WALKERS = 10;
	private static int lengthOfWalk;
	private static final int DEFAULT_LENGTH_OF_WALK = 6;
	
	public RandomWalkVertex(CommandLine line) {
		String otherOptsStr = line.getOptionValue(GPSNodeRunner.OTHER_OPTS_OPT_NAME);
		System.out.println("otherOptsStr: " + otherOptsStr);
		initialNumWalkers = DEFAULT_NUM_WALKERS;
		lengthOfWalk = DEFAULT_LENGTH_OF_WALK;
		if (otherOptsStr != null) {
			String[] split = otherOptsStr.split("###");
			for (int index = 0; index < split.length; ) {
				String flag = split[index++];
				String value = split[index++];
				if ("-nw".equals(flag)) {
					initialNumWalkers = Integer.parseInt(value);
					System.out.println("initialNumWalkers: " + initialNumWalkers);
				} else if ("-lw".equals(flag)) {
					lengthOfWalk = Integer.parseInt(value);
					System.out.println("lengthOfWalk: " + lengthOfWalk);
				}
			}
		}
	}

	@Override
	public void compute(Iterable<IntWritable> messageValues, int superstepNo) {
		int sum = initialNumWalkers;
		if (superstepNo > 1) {
			sum = 0;
			int numMessages = 0;
			for (IntWritable messageValue : messageValues) {
				numMessages++;
				sum += messageValue.getValue();
			}
		}
		sendMessages(sum, getNeighborIds(), superstepNo);
		if (superstepNo > lengthOfWalk) {
			voteToHalt();
		}
		setValue(new IntWritable(sum));
	}

	public void sendMessages(int numMessagesToSend, int[] neighborIds, int superstepNo) {
//		if (numMessagesToSend > 100) {
//			System.out.println("numMessagesToSend: " + numMessagesToSend);
//		}
		if (messagesToNeighbors.length < neighborIds.length) {
			messagesToNeighbors = new int[neighborIds.length];
		} else {
			for (int i = 0; i < neighborIds.length; ++i) {
				messagesToNeighbors[i] = 0;
			}
		}	
		getGlobalObjectsMap().putOrUpdateGlobalObject(
			"num-walkers-" + superstepNo, new LongSumGlobalObject((long) numMessagesToSend));
		Random random = new Random();
		int neighborsLength = neighborIds.length;
		if (neighborsLength == 0) {
			for (int i = 0; i < numMessagesToSend; ++i) {
				sendMessage(random.nextInt(getGraphSize()), new IntWritable(1));
			}
			return;
		} else {
			for (int i = 0; i < numMessagesToSend; ++i) {
				int neighborIdIndex = random.nextInt(neighborsLength);
//				sendMessage(neighborIds[random.nextInt(neighborsLength)],
//					new IntWritable(1));
				messagesToNeighbors[neighborIdIndex] += 1;
			}
			for (int i = 0; i < neighborsLength; ++i) {
				if (messagesToNeighbors[i] > 0) {
					sendMessage(neighborIds[i], new IntWritable(messagesToNeighbors[i]));
				}
			}
		}
	}

	@Override
	public IntWritable getInitialValue(int id) {
		return new IntWritable(400);//getNeighborsSize());
	}

	/**
	 * Factory class for {@link RandomWalkVertex}.
	 * 
	 * @author semihsalihoglu
	 */
	public static class RandomWalkVertexFactory extends NullEdgeVertexFactory<IntWritable, IntWritable> {

		@Override
		public NullEdgeVertex<IntWritable, IntWritable> newInstance(CommandLine commandLine) {
			return new RandomWalkVertex(commandLine);
		}
	}
	
	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return RandomWalkVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return RandomWalkVertex.class;
		}

		@Override
		public Class<?> getVertexValueClass() {
			return IntWritable.class;
		}

		@Override
		public Class<?> getMessageValueClass() {
			return IntWritable.class;
		}
	}
}
