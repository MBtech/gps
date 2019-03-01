package gps.examples.handwrittenrandombipartitematching;

import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.mina.core.buffer.IoBuffer;

import gps.globalobjects.IntSumGlobalObject;
import gps.graph.Edge;
import gps.graph.Master;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.node.GPSJobConfiguration;
import gps.node.GPSNodeRunner;
import gps.node.Utils;
import gps.writable.IntWritable;
import gps.writable.MinaWritable;
import gps.writable.NullWritable;

/**
 * Handwritten random bipartite matching algorithm from Pregel.
 * 
 * @author semihsalihoglu
 */
public class RandomBipartiteMatchingVertex extends NullEdgeVertex<
	RandomBipartiteMatchingVertex.VertexData, IntWritable>{

	public RandomBipartiteMatchingVertex() {
	}
    
	@Override
	public void compute(Iterable<IntWritable> messageValues, int superstepNo) {
		if ((superstepNo % 4) == 1) {
			// System.out.println("superstepNo: 1: " + getId());
			if (getValue().isLeft && (getValue().match == -1)) {
				// System.out.println(getId() + " is sending an invitation message to all its neighbors.");
				for(int neighborId : getNeighborIds()) {
					// System.out.println(getId() + " is sending an invitation message to: "
//						+ neighborId);
					sendMessage(neighborId, new IntWritable(getId()));
				}
//				sendMessages(getNeighborIds(), new IntWritable(getId()));
			}
		} else if ((superstepNo % 4) == 2) {
			// System.out.println("superstepNo: 2: " + getId());
			if (!getValue().isLeft && (getValue().match == -1)) {
				int acceptedLeftNode = -1;
				for (IntWritable message : messageValues) {
					acceptedLeftNode = message.getValue();
				}
				if (acceptedLeftNode != -1) {
					// System.out.println(getId() + " is sending an acceptance message to: "
//						+ acceptedLeftNode);
					sendMessage(acceptedLeftNode, new IntWritable(getId()));
				}
			}
		} else if ((superstepNo % 4) == 3) {
			// System.out.println("superstepNo: 3: " + getId());
			if (getValue().isLeft && (getValue().match == -1)) {
				int notifiedRightNode = -1;
				for (IntWritable message : messageValues) {
					notifiedRightNode = message.getValue();
				}
				if (notifiedRightNode != -1) {
					getValue().match = notifiedRightNode;
					// System.out.println(getId() + " is sending a notification message to: "
//						+ notifiedRightNode);
					sendMessage(notifiedRightNode, new IntWritable(getId()));
				}
			}
		} else if ((superstepNo % 4) == 0) {
			// System.out.println("superstepNo: 4: " + getId());
			if (!getValue().isLeft) {
				for (IntWritable message : messageValues) {
					getValue().match = message.getValue();
					getGlobalObjectsMap().putOrUpdateGlobalObject("nmv", new IntSumGlobalObject(1));
				}
			}
		}
	}

    @Override
    public VertexData getInitialValue(int id) {
        return new VertexData((id % 2) == 0);
    }

	public static class RandomBipartiteMatchingVertexFactory extends NullEdgeVertexFactory<
	RandomBipartiteMatchingVertex.VertexData, IntWritable> {

		@Override
		public NullEdgeVertex<RandomBipartiteMatchingVertex.VertexData, IntWritable> newInstance(
			CommandLine commandLine) {
			return new RandomBipartiteMatchingVertex();
		}
	}
    
    public static class RandomBipartiteMatchingMaster extends Master {
    	public static int DEFAULT_NUM_MAX_ITERATIONS = 4;

    	public int numMaxIterations;
    	private int iterationNo;
    	private int numMatchedVertices;
    	public RandomBipartiteMatchingMaster(CommandLine commandLine) {
    		parseOtherOpts(commandLine);
    		this.iterationNo = 1;
    		this.numMatchedVertices = 0;
    	}
    	
    	@Override
    	public void compute(int superstepNo) {
			// System.out.println("master first line superstepNo: " + superstepNo);
    		if (superstepNo == 1) {
    			// System.out.println("master superstepNo: 1: ");
    			return;
    		}
    		if ((superstepNo % 4) == 1) {
    			numMatchedVertices += ((IntSumGlobalObject) getGlobalObjectsMap().getGlobalObject("nmv")).getValue().getValue();
    			getGlobalObjectsMap().clearNonDefaultObjects();
    			if (iterationNo > numMaxIterations) {
        			// System.out.println("master finishing computation: ");
    				this.continueComputation = false;
    			}
    		}
    		if ((superstepNo % 4) == 0) {
    			// System.out.println("master superstepNo: 4: ");
    			getGlobalObjectsMap().putGlobalObject("nmv", new IntSumGlobalObject(0));
    			iterationNo++;
    		} 
    	}

    	private void parseOtherOpts(CommandLine line) {
    		String otherOptsStr = line.getOptionValue(GPSNodeRunner.OTHER_OPTS_OPT_NAME);
    		// System.out.println("otherOptsStr: " + otherOptsStr);
    		numMaxIterations = DEFAULT_NUM_MAX_ITERATIONS;
    		if (otherOptsStr != null) {
    			String[] split = otherOptsStr.split("###");
    			for (int index = 0; index < split.length; ) {
    				String flag = split[index++];
    				String value = split[index++];
    				if ("-max".equals(flag)) {
    					numMaxIterations = Integer.parseInt(value);
    					// System.out.println("numMaxIterations: " + numMaxIterations);
    				}
    			}
    		}
    	}
    	
    	@Override
    	public void writeOutput(BufferedWriter bw) throws IOException {
    		bw.write("numMatchedVertices\t" + numMatchedVertices + "\n");
    		super.writeOutput(bw);
    	}

    }

    public static class VertexData extends MinaWritable {
        // properties
        boolean isLeft;
        int match;
        public VertexData(boolean isLeft) {
        	this.isLeft = isLeft;
        	this.match = -1;
		}
        @Override
        public int numBytes() {return 5;}
        @Override
        public void write(IoBuffer IOB) {
            IOB.put(isLeft?(byte)1:(byte)0);
            IOB.putInt(match);
        }
        @Override
        public void read(IoBuffer IOB) {
            isLeft= IOB.get()==0?false:true;
            match= IOB.getInt();
        }
        @Override
        public int read(byte[] _BA, int _idx) {
            isLeft= Utils.byteArrayToBooleanBigEndian(_BA, _idx + 0);
            match= Utils.byteArrayToIntBigEndian(_BA, _idx + 1);
            return 5;
        }
        @Override
        public int read(IoBuffer IOB, byte[] _BA, int _idx) {
            IOB.get(_BA, _idx, 5);
            return 5;
        }
        @Override
        public void combine(byte[] _MQ, byte [] _tA) {
             // do nothing
        }
        @Override
        public String toString() {
            return "" + "Match: " + match;
        }
    }
	
	public static class JobConfiguration extends GPSJobConfiguration {

		@Override
		public Class<?> getVertexFactoryClass() {
			return RandomBipartiteMatchingVertexFactory.class;
		}

		@Override
		public Class<?> getVertexClass() {
			return RandomBipartiteMatchingVertex.class;
		}

		@Override
		public Class<?> getVertexValueClass() {
			return VertexData.class;
		}

		@Override
		public Class<?> getMessageValueClass() {
			return IntWritable.class;
		}

		@Override
		public Class<?> getMasterClass() {
			return RandomBipartiteMatchingMaster.class;
		}
	}
}
