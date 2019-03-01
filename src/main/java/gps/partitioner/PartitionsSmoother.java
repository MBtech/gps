package gps.partitioner;

import gps.node.Pair;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class PartitionsSmoother {

	public static String INPUT_FILE_OPT_NAME = "if";
	public static String OUTPUT_FILE_OPT_NAME = "of";
	public static String NUM_PARTITIONS_OPT_NAME = "np";
	public static String NUM_VERTICES_DIFFERENCE_THRESHOLD_OPT_NAME = "nvdt";
	public static int DEFAULT_NUM_VERTICES_DIFFERENCE_THRESHOLD = 1000;
	public static String NUM_VERTICES_TO_TRANSFER_PER_ITERATION_OPT_NAME = "nvttpi";
	public static int DEFAULT_NUM_VERTICES_TO_TRANSFER_PER_ITERATION = 1000;
	public static String NUM_EDGES_DIFFERENCE_THRESHOLD_OPT_NAME = "nedt";
	public static int DEFAULT_NUM_EDGES_DIFFERENCE_THRESHOLD = 500000;
	public static String NUM_VERTICES_TO_EXCHANGE_PER_ITERATION_OPT_NAME = "nvtepi";
	public static int DEFAULT_NUM_VERTICES_TO_EXCHANGE_PER_ITERATION = 1000;
	public static String NUM_BUCKETS_OPT_NAME = "nb";
	public static int DEFAULT_NUM_BUCKETS = 7;
	public static String SMOOTH_OPT_NAME = "s";
	public static boolean DEFAULT_SMOOTH = true;
	public static String OPT_MAPPING_FILE_SMOOTH_OPT_NAME = "mf";
	public static String OPT_NUM_VERTICES_UPPER_BOUND_OPT_NAME = "nv";
	
	public static void smoothPartitionsAndOutput(String inputGraphFile, int numPartitions,
		String outputIdsFile, int numVerticesDifferenceThreshold,
		int numVerticesToTransferPerIteration, int numEdgesDifferenceThreshold,
		int numVerticesToExchangePerIteration, int numBuckets, boolean smooth,
		String optMappingFile, int optNumVertices) throws NumberFormatException, IOException {
		List<Partition> partitions = getInitialPartitions(inputGraphFile,
			numPartitions, numBuckets, optMappingFile, optNumVertices);
//		int avgNumVertices = getAverageNumVertices(partitions);
//		System.out.println("avgNumVertices: " + avgNumVertices);
		if (smooth) {
			smoothAccordingToNumVertices(partitions, numVerticesDifferenceThreshold, numVerticesToTransferPerIteration);
			smoothAccordingToNumEdges(partitions, numEdgesDifferenceThreshold, numVerticesToExchangePerIteration);
			writeFinalIds(partitions, outputIdsFile);
		}
		dumpPartitions(partitions);
	}

	private static void smoothAccordingToNumEdges(List<Partition> partitions, int numEdgesDifferenceThreshold, int numVerticesToExchangePerIteration) {
		VertexWrapper maxTmpVertexWrapper;
		VertexWrapper minTmpVertexWrapper;
		int tmpInt;
		int iterationNo = 0;
		int previousMaxNumEdgesPartitionId = -1;
		int previousMinNumEdgesPartitionId = -1;
		while (true) {
			iterationNo++;
			System.out.println("Starting a new iteration: " + iterationNo);
			Pair<Integer, Integer> maxAndMinPartitionIds =
				findMaxAndMinNumEdgesPartition(partitions);
			int maxNumEdgesPartitionId = maxAndMinPartitionIds.fst;
			int maxNumEdges = partitions.get(maxNumEdgesPartitionId).numEdges;
			int minNumEdgesPartitionId = maxAndMinPartitionIds.snd;
			int minNumEdges = partitions.get(minNumEdgesPartitionId).numEdges;
			if (previousMaxNumEdgesPartitionId == 1) {
				previousMaxNumEdgesPartitionId = maxNumEdgesPartitionId;
				previousMinNumEdgesPartitionId = minNumEdgesPartitionId;
			} else {
				if ((previousMaxNumEdgesPartitionId == minNumEdgesPartitionId) &&
					(previousMinNumEdgesPartitionId == maxNumEdgesPartitionId)) {
					System.out.println("Caught in a min-max infinite loop. Breaking");
					break;
				} else {
					previousMaxNumEdgesPartitionId = maxNumEdgesPartitionId;
					previousMinNumEdgesPartitionId = minNumEdgesPartitionId;
				}
			}
//			System.out.println("maxNumEdgesPartitionId: " + maxNumEdgesPartitionId
//				+ " maxNumEdges: " + maxNumEdges);
//			System.out.println("minNumEdgesPartitionId: " + minNumEdgesPartitionId
//				+ " minNumEdges: " + minNumEdges);
			// TODO change these thresholds
			boolean breakFromOuterLoop = false;
			if ((maxNumEdges - minNumEdges) > numEdgesDifferenceThreshold) {
				for (int i = 0; i < numVerticesToExchangePerIteration; ++i) {
					maxTmpVertexWrapper = partitions.get(maxNumEdgesPartitionId)
						.removeAVertexFromLargestBucket();
					minTmpVertexWrapper = partitions.get(minNumEdgesPartitionId)
						.removeAVertexFromSmallestBucket();
					tmpInt = maxTmpVertexWrapper.finalVertexId;
					maxTmpVertexWrapper.finalVertexId = minTmpVertexWrapper.finalVertexId;
					minTmpVertexWrapper.finalVertexId = tmpInt;
					partitions.get(maxNumEdgesPartitionId).addVertexWrapper(minTmpVertexWrapper,
						false /* is not transferred */);
					partitions.get(minNumEdgesPartitionId).addVertexWrapper(maxTmpVertexWrapper,
						false /* is not transferred */);
//					System.out.println("exchanging vertexId: " + maxTmpVertexWrapper.originalVertexId
//						+ " fromPartition: " + maxNumEdgesPartitionId + " numEdges: " + maxTmpVertexWrapper.numEdges
//						+ " with vertexId: " + minTmpVertexWrapper.originalVertexId
//						+ " fromPartitionId:" + minNumEdgesPartitionId + " numEdges: " + minTmpVertexWrapper.numEdges);
					if (maxTmpVertexWrapper.numEdges <= minTmpVertexWrapper.numEdges) {
						System.out.println("Num edges are not going down. Breaking.");	
						breakFromOuterLoop = true;
						break;
					}
				}
				if (breakFromOuterLoop) {
					break;
				}
			} else {
				System.out.println(
					"Edge difference between the maxEdge and the minEdge partitions is: "
					+ (maxNumEdges - minNumEdges) + " (larger than threshold, breaking)");
				break;
			}
		}
	}


	private static void writeFinalIds(List<Partition> partitions, String outputIdsFile) throws IOException {
		BufferedWriter bufferedWriter = PartitionerUtils.getBufferedWriter(outputIdsFile);
		for (Partition partition : partitions) {
			for (List<VertexWrapper> vertexWrapperList : partition.vertexWrapperBuckets) {
				for (VertexWrapper vertexWrapper : vertexWrapperList) {
					bufferedWriter.write("-1\t" + vertexWrapper.originalVertexId + " " + vertexWrapper.finalVertexId + "\n");
				}
			}
		}
		bufferedWriter.close();
	}

	private static void smoothAccordingToNumVertices(List<Partition> partitions, int numVerticesDifferenceThreshold, int numVerticesToTransferPerIteration) {
		VertexWrapper tmpVertexWrapper;
		int iterationNo = 0;
		while (true) {
			iterationNo++;
			System.out.println("Starting a new iteration: " + iterationNo);
			Pair<Integer, Integer> maxAndMinPartitionIds =
				findMaxAndMinNumVerticesPartition(partitions);
			int maxNumVerticesPartitionId = maxAndMinPartitionIds.fst;
			int maxNumVertices = partitions.get(maxNumVerticesPartitionId).numVertices;
			int minNumVerticesPartitionId = maxAndMinPartitionIds.snd;
			int minNumVertices = partitions.get(minNumVerticesPartitionId).numVertices;
//			System.out.println("maxNumVerticesPartitionId: " + maxNumVerticesPartitionId
//				+ " maxNumVertices: " + maxNumVertices);
//			System.out.println("minNumVerticesPartitionId: " + minNumVerticesPartitionId
//				+ " minNumVertices: " + minNumVertices);
			// TODO change these threasholds
			if ((maxNumVertices - minNumVertices) > numVerticesDifferenceThreshold) {
				for (int i = 0; i < numVerticesToTransferPerIteration; ++i) {
					tmpVertexWrapper = partitions.get(maxNumVerticesPartitionId)
						.removeAVertexFromLargestBucket();
					partitions.get(minNumVerticesPartitionId).addVertexWrapper(tmpVertexWrapper,
						true /* is transferred */);
//					System.out.println("transfering vertexId: " + tmpVertexWrapper.originalVertexId
//						+ " fromPartition: " + maxNumVerticesPartitionId
//						+ " toPartition: " + minNumVerticesPartitionId);
				}
			} else {
				System.out.println(
					"Vertex difference between the maxVertex and the minVertex partitions is: "
					+ (maxNumVertices - minNumVertices) + " (larger than threshold, breaking)");
				break;
			}
		}
	}

	private static Pair<Integer, Integer> findMaxAndMinNumEdgesPartition(
		List<Partition> partitions) {
		int minNumEdgesPartitionId = 0;
		int minNumEdges = partitions.get(0).numEdges;
		int maxNumEdgesPartitionId = 0;
		int maxNumEdges = partitions.get(0).numEdges;
		Partition tmpPartition = null;
		for (int i = 1; i < partitions.size(); ++i) {
			tmpPartition = partitions.get(i);
			if (tmpPartition.numEdges > maxNumEdges) {
				maxNumEdges = tmpPartition.numEdges;
				maxNumEdgesPartitionId = i;
			} else if (tmpPartition.numEdges < minNumEdges) {
				minNumEdges = tmpPartition.numEdges;
				minNumEdgesPartitionId = i;
			}
		}
		return Pair.of(maxNumEdgesPartitionId, minNumEdgesPartitionId);
	}

	private static Pair<Integer, Integer> findMaxAndMinNumVerticesPartition(
		List<Partition> partitions) {
		int minNumVerticesPartitionId = 0;
		int minNumVertices = partitions.get(0).numVertices;
		int maxNumVerticesPartitionId = 0;
		int maxNumVertices = partitions.get(0).numVertices;
		Partition tmpPartition = null;
		for (int i = 1; i < partitions.size(); ++i) {
			tmpPartition = partitions.get(i);
			if (tmpPartition.numVertices > maxNumVertices) {
				maxNumVertices = tmpPartition.numVertices;
				maxNumVerticesPartitionId = i;
			} else if (tmpPartition.numVertices < minNumVertices) {
				minNumVertices = tmpPartition.numVertices;
				minNumVerticesPartitionId = i;
			}
		}
		return Pair.of(maxNumVerticesPartitionId, minNumVerticesPartitionId);
	}

	private static int getAverageNumVertices(List<Partition> partitions) {
		int totalNumVertices = 0;
		for (Partition partition : partitions) {
			totalNumVertices += partition.numVertices;
		}
		return (totalNumVertices / partitions.size() + 1);
	}

	private static int getAverageNumEdges(List<Partition> partitions) {
		int totalNumEdges = 0;
		for (Partition partition : partitions) {
			totalNumEdges += partition.numEdges;
		}
		return (totalNumEdges / partitions.size() + 1);
	}

	private static List<Partition> getInitialPartitions(String inputGraphFile,
		int numPartitions, int numBuckets, String optOutputMappingFile, int optNumVertices) throws IOException {
		int[] mappingsArray = null;
		if (optOutputMappingFile != null) {
			mappingsArray = getMappings(optOutputMappingFile, optNumVertices);			
		}
		return getInitialPartitions(inputGraphFile, numPartitions, numBuckets, mappingsArray);
	}


	private static int[] getMappings(String optOutputMappingFile, int numVertices) throws IOException {
		int[] mappingsArray = new int[numVertices];
		for (int i = 0; i < mappingsArray.length; ++i) {
			mappingsArray[i] = i;
		}
		BufferedReader bufferedReader = PartitionerUtils.getBufferedReader(optOutputMappingFile);
		String line = null;
		int counter = 0;
		String[] split = null;
		long previousTime = System.currentTimeMillis();
		int vertexId;
		int mappedVertexId;
		while ((line = bufferedReader.readLine()) != null) {
			try {
				counter++;
				split = line.split("\\s+");
				if (counter % 1000000 == 0) {
					long currentTime = System.currentTimeMillis();
					System.out.println("Parsed " + counter + " lines. Time: "
						+ (currentTime - previousTime));
					previousTime = System.currentTimeMillis();
				}
//				System.out.println("line: " + line);
				vertexId = Integer.parseInt(split[1]);
				mappedVertexId = Integer.parseInt(split[2]);
				mappingsArray[vertexId] = mappedVertexId;
			} catch (NumberFormatException e) {
				// Do nothing;
			}
		}
		bufferedReader.close();
		return mappingsArray;
	}

	private static List<Partition> getInitialPartitions(String inputGraphFile, int numPartitions,
		int numBuckets, int[] optMappingFile) throws FileNotFoundException, IOException {
		List<Partition> partitions = new ArrayList<PartitionsSmoother.Partition>();
		for (int i = 0; i < numPartitions; ++i) {
			partitions.add(new Partition(i /* id */, numBuckets, numPartitions));
		}

		BufferedReader bufferedReader = PartitionerUtils.getBufferedReader(inputGraphFile);
		String line = null;
		int counter = 0;
		String[] split = null;
		long previousTime = System.currentTimeMillis();
		int vertexId;
		long numEdges = 0;
		int neighborsPartitionId;
		int neighborId;
		int vertexsPartitionId;
		while ((line = bufferedReader.readLine()) != null) {
			try {
				counter++;
				split = line.split("\\s+");
				if (counter % 1000000 == 0) {
					long currentTime = System.currentTimeMillis();
					System.out.println("Parsed " + counter + " lines. Time: "
						+ (currentTime - previousTime));
					previousTime = System.currentTimeMillis();
				}
//				System.out.println("line: " + line);
				vertexId = Integer.parseInt(split[0]);
				if (optMappingFile != null) {
					vertexId = optMappingFile[vertexId];
				}
				vertexsPartitionId = vertexId % numPartitions;
				partitions.get(vertexsPartitionId).addVertexWrapper(
					new VertexWrapper(vertexId, split.length - 1 /* num edges */),
					false /* not transferred */);
				numEdges += split.length - 1;
				for (int i = 1; i < split.length; ++i) {
					neighborId = Integer.parseInt(split[i]);
					if (optMappingFile != null) {
						neighborId = optMappingFile[neighborId];
					}
					neighborsPartitionId = neighborId % numPartitions;
					partitions.get(neighborsPartitionId).addIncomingEdgeToAdjustMaxId(neighborId);
					if (neighborsPartitionId != vertexsPartitionId) {
						partitions.get(neighborsPartitionId).numIncomingEdges++;
						partitions.get(vertexsPartitionId).numOutgoingEdges++;
					}
				}
			} catch (NumberFormatException e) {
				// Do nothing;
			}
		}
		bufferedReader.close();
		dumpPartitions(partitions);
		return partitions;
	}

	private static void dumpPartitions(List<Partition> partitions) {
		for (Partition partition : partitions) {
			partition.dumpPartition();
		}
		System.out.println("Dumping partition statistics in excel format.");
		int counter = 1;
		for (Partition partition : partitions) {
			System.out.println((counter++) + "\t" + partition.numVertices + "\t"
				+ partition.numEdges + "\t" + partition.numIncomingEdges
				+ "\t" + partition.numOutgoingEdges);
		}
		System.out.println("End of dumping partition statistics in excel format.");
	}
	
	public static class Partition {
		int numEdges;
		int numVertices;
		int maxIdVertexId = -1;
		int numIncomingEdges;
		int numOutgoingEdges;
		List<List<VertexWrapper>> vertexWrapperBuckets = new ArrayList<List<VertexWrapper>>();
		private final int numBuckets;
		private final int id;
		private final int numPartitions;

		public Partition(int id, int numBuckets, int numPartitions) {
			this.id = id;
			this.numBuckets = numBuckets;
			this.numPartitions = numPartitions;
			this.numEdges = 0;
			this.numVertices = 0;
			this.numIncomingEdges = 0;
			this.numOutgoingEdges = 0;
			for (int i = 0; i < numBuckets; ++i) {
				vertexWrapperBuckets.add(new ArrayList<PartitionsSmoother.VertexWrapper>());
			}
		}

		public VertexWrapper removeAVertexFromSmallestBucket() {
			for (int i = 0; i < numBuckets; ++i) {
				List<VertexWrapper> tmpBucket = vertexWrapperBuckets.get(i);
				if (tmpBucket.size() > 0) {
					VertexWrapper retVal = tmpBucket.remove(tmpBucket.size() - 1);
					this.numVertices--;
					this.numEdges -= retVal.numEdges;
					return retVal;
				}
			}
			return null;
		}
		
		public VertexWrapper removeAVertexFromLargestBucket() {
			for (int i = numBuckets -1; i >=0; --i) {
				List<VertexWrapper> tmpBucket = vertexWrapperBuckets.get(i);
				if (tmpBucket.size() > 0) {
					VertexWrapper retVal = tmpBucket.remove(tmpBucket.size() - 1);
					this.numVertices--;
					this.numEdges -= retVal.numEdges;
					return retVal;
				}
			}
			return null;
		}
		
		public void addIncomingEdgeToAdjustMaxId(int incomingEdgeId) {
			if (incomingEdgeId > maxIdVertexId) {
				maxIdVertexId = incomingEdgeId;
			}
		}

		public void addVertexWrapper(VertexWrapper vertexWrapper,
			boolean isTransferredFromAnotherPartition) {
			int bucketId = getNumberOfDigits(vertexWrapper.numEdges) - 1;
			if (bucketId >= numBuckets) {
				System.out.println("bucketId: " + bucketId
					+ " is larger than or equal to numBuckets: " + numBuckets
					+ " assigning to largest bucketId: " + (numBuckets - 1));
				bucketId = numBuckets - 1;
			}
			vertexWrapperBuckets.get(bucketId).add(vertexWrapper);
			this.numVertices++;
			this.numEdges += vertexWrapper.numEdges;
			if (isTransferredFromAnotherPartition) {
				this.maxIdVertexId += numPartitions;
				vertexWrapper.finalVertexId = this.maxIdVertexId;
			} else {
				if (vertexWrapper.finalVertexId > this.maxIdVertexId) {
					this.maxIdVertexId = vertexWrapper.originalVertexId;
				}
			}
		}

		private int getNumberOfDigits(int positiveInt) {
			for (int i = 1; ; ++i) {
				if (positiveInt < Math.pow(10, i)) {
					return i;
				}
			}
		}
		
		void dumpPartition() {
			System.out.println("Dumping Partition with id: " + this.id);
			System.out.println("numVertices: " + numVertices);
			System.out.println("numEdges: " + numEdges);
			System.out.println("numIncomingEdges: " + numIncomingEdges);
			System.out.println("numOutgoingEdges: " + numOutgoingEdges);			
			System.out.println("maxIdVertexId: " + maxIdVertexId);
//			for (int i = 0; i < numBuckets; ++i) {
//				System.out.println("Dumping bucket id: " + i);
//				List<VertexWrapper> vertexWrapperList = vertexWrapperBuckets.get(i);
//				for (VertexWrapper vertexWrapper : vertexWrapperList) {
//					vertexWrapper.dumpVertexWrapper();
//				}
//			}
		}
	}

	public static class VertexWrapper {
		int originalVertexId;
		int numEdges;
		int finalVertexId;
		
		public VertexWrapper(int vertexId, int numEdges) {
			this.originalVertexId = vertexId;
			this.numEdges = numEdges;
			this.finalVertexId = vertexId;
		}
		
		void dumpVertexWrapper() {
			System.out.println("vertexId: " + this.originalVertexId + " numEdges: "
				+ this.numEdges + " finalVertexId: " + this.finalVertexId);

		}
	}
	public static void main(String[] args) throws NumberFormatException, IOException {
		CommandLine line = parseAndAssertCommandLines(args);
		String inputFile = line.getOptionValue(INPUT_FILE_OPT_NAME);
		int numPartitions = Integer.parseInt(line.getOptionValue(NUM_PARTITIONS_OPT_NAME));
		String outputFile = line.getOptionValue(OUTPUT_FILE_OPT_NAME);
		int numVerticesDifferenceThreshold = DEFAULT_NUM_VERTICES_DIFFERENCE_THRESHOLD;
		if (line.hasOption(NUM_VERTICES_DIFFERENCE_THRESHOLD_OPT_NAME)) {
			numVerticesDifferenceThreshold = Integer.parseInt(line.getOptionValue(NUM_VERTICES_DIFFERENCE_THRESHOLD_OPT_NAME));
		}
		int numVerticesToTransferPerIteration = DEFAULT_NUM_VERTICES_TO_TRANSFER_PER_ITERATION;
		if (line.hasOption(NUM_VERTICES_TO_TRANSFER_PER_ITERATION_OPT_NAME)) {
			numVerticesToTransferPerIteration = Integer.parseInt(line.getOptionValue(NUM_VERTICES_TO_TRANSFER_PER_ITERATION_OPT_NAME));
		}
		int numEdgesDifferenceThreshold = DEFAULT_NUM_EDGES_DIFFERENCE_THRESHOLD;
		if (line.hasOption(NUM_EDGES_DIFFERENCE_THRESHOLD_OPT_NAME)) {
			numEdgesDifferenceThreshold = Integer.parseInt(line.getOptionValue(NUM_EDGES_DIFFERENCE_THRESHOLD_OPT_NAME));
		}
		int numVerticesToExchangePerIteration = DEFAULT_NUM_VERTICES_TO_EXCHANGE_PER_ITERATION;
		if (line.hasOption(NUM_VERTICES_TO_EXCHANGE_PER_ITERATION_OPT_NAME)) {
			numVerticesToExchangePerIteration = Integer.parseInt(line.getOptionValue(NUM_VERTICES_TO_EXCHANGE_PER_ITERATION_OPT_NAME));
		}
		int numBuckets = DEFAULT_NUM_BUCKETS;
		if (line.hasOption(NUM_BUCKETS_OPT_NAME)) {
			numBuckets = Integer.parseInt(line.getOptionValue(NUM_BUCKETS_OPT_NAME));
		}
		boolean smooth = DEFAULT_SMOOTH;
		if (line.hasOption(SMOOTH_OPT_NAME)) {
			smooth = Boolean.parseBoolean(line.getOptionValue(SMOOTH_OPT_NAME));
		}
		String optMappingFile = line.getOptionValue(OPT_MAPPING_FILE_SMOOTH_OPT_NAME);
		int optNumVertices = -1;
		if (line.hasOption(OPT_NUM_VERTICES_UPPER_BOUND_OPT_NAME)) {
			optNumVertices = Integer.parseInt(line.getOptionValue(OPT_NUM_VERTICES_UPPER_BOUND_OPT_NAME));
		}
		System.out.println("Printing command line arguments...");
		System.out.println("inputFile: " + inputFile);
		System.out.println("outputFile: " + outputFile);
		System.out.println("numPartitions: " + numPartitions);
		System.out.println("numVerticesDifferenceThreshold: " + numVerticesDifferenceThreshold);
		System.out.println("numVerticesToTransferPerIteration: " + numVerticesToTransferPerIteration);
		System.out.println("numEdgesDifferenceThreshold: " + numEdgesDifferenceThreshold);
		System.out.println("numVerticesToExchangePerIteration: " + numVerticesToExchangePerIteration);
		System.out.println("numBuckets: " + numBuckets);
		System.out.println("optMappingFile: " + optMappingFile);
		System.out.println("optNumVertices: " + optNumVertices);
		System.out.println("End of printing command line arguments...");
		smoothPartitionsAndOutput(inputFile, numPartitions, outputFile, numVerticesDifferenceThreshold,
			numVerticesToTransferPerIteration, numEdgesDifferenceThreshold,
			numVerticesToExchangePerIteration, numBuckets, smooth, optMappingFile, optNumVertices);
	}

	private static CommandLine parseAndAssertCommandLines(String[] args) {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption(INPUT_FILE_OPT_NAME, INPUT_FILE_OPT_NAME, true, "input file");
		options.addOption(OUTPUT_FILE_OPT_NAME, OUTPUT_FILE_OPT_NAME, true, "output file");
		options.addOption(NUM_PARTITIONS_OPT_NAME, NUM_PARTITIONS_OPT_NAME, true, "num partitions");
		options.addOption(NUM_VERTICES_DIFFERENCE_THRESHOLD_OPT_NAME,
			NUM_VERTICES_DIFFERENCE_THRESHOLD_OPT_NAME, true, "num vertices difference threshold");
		options.addOption(NUM_VERTICES_TO_TRANSFER_PER_ITERATION_OPT_NAME,
			NUM_VERTICES_TO_TRANSFER_PER_ITERATION_OPT_NAME, true, "num vertices to transfer per iteration");
		options.addOption(NUM_EDGES_DIFFERENCE_THRESHOLD_OPT_NAME,
			NUM_EDGES_DIFFERENCE_THRESHOLD_OPT_NAME, true, "num edges difference threshold");
		options.addOption(NUM_VERTICES_TO_EXCHANGE_PER_ITERATION_OPT_NAME,
			NUM_VERTICES_TO_EXCHANGE_PER_ITERATION_OPT_NAME, true, "num vertices to exchange per iteration. this is done for balancing the number of edges");
		options.addOption(NUM_BUCKETS_OPT_NAME, NUM_BUCKETS_OPT_NAME, true, "num buckets (if 2 " +
			"vertices will be bucketed into 2 groups (v \\in [1,10) and v \\in [10, 100), etc..)");
		options.addOption(SMOOTH_OPT_NAME, SMOOTH_OPT_NAME, true, "whether or not to smooth the partitions or to just dump the statistics");
		options.addOption(OPT_MAPPING_FILE_SMOOTH_OPT_NAME, OPT_MAPPING_FILE_SMOOTH_OPT_NAME, true, "optional mapping file");
		options.addOption(OPT_NUM_VERTICES_UPPER_BOUND_OPT_NAME, OPT_NUM_VERTICES_UPPER_BOUND_OPT_NAME, true, "optional number of vertices upper bound to keep an array instead of a map");
		try {
			CommandLine line = parser.parse(options, args);
			return line;
		} catch (ParseException e) {
			System.err.println("Unexpected exception:" + e.getMessage());
			System.exit(-1);
			return null;
		}
	}

}
