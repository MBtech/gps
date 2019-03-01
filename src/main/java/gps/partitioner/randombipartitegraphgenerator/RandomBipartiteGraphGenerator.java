package gps.partitioner.randombipartitegraphgenerator;

import gps.partitioner.PartitionerUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * A simple random bipartite graph generator. All edges will be from vertices
 * with even ids {0, 2, 4,...} to vertices with odd ids {1, 3, 5,...}. Number of
 * edges should be given as an even integer number.
 * 
 * @author semihsalihoglu
 */
public class RandomBipartiteGraphGenerator {

	public static void main(String[] args) throws IOException {
		long beginningTime = System.currentTimeMillis();
		CommandLine line = parseAndAssertCommandLines(args);
		String outputDirectory = line.getOptionValue("od");
		String outputFilePrefix = line.getOptionValue("ofp");
		int numberOfOutputFiles = Integer.parseInt(line.getOptionValue("nof"));
		int numVertices = Integer.parseInt(line.getOptionValue("nv"));
		int numEvenVertices = numVertices/2;
		long numEdges = Long.parseLong(line.getOptionValue("ne"));
		long averageNumberOfEdges = numEdges/numEvenVertices;

		System.out.println("averageNumEdges:" + averageNumberOfEdges);
		int[] numEvenVerticesEdgeIndexes = new int[numEvenVertices];
		int[][] edges = new int[numEvenVertices][];
		for (int i = 0; i < edges.length; ++i) {
			edges[i] = new int[(int) averageNumberOfEdges];
		}
		Random random = new Random();
		int sourceId, destinationId;
		int sourceIdNumEdges;
		int[] tmpEdges;
		for (int i = 0; i < numEdges; ++i) {
			if ((i % 50000000) == 0) {
				System.out.println("generated " + i + "th edge.");
			} 
			sourceId = random.nextInt(numEvenVertices) * 2;
			destinationId = (random.nextInt(numEvenVertices) * 2) + 1;
//			System.out.println("adding an edge from: " + sourceId + " to: " + destinationId);
			sourceIdNumEdges = numEvenVerticesEdgeIndexes[sourceId/2];
			tmpEdges = edges[sourceId/2];
			if (sourceIdNumEdges == tmpEdges.length) {
				int newEdgeArrayLength =
					tmpEdges.length > 500 ? (int) (tmpEdges.length * 1.2) : tmpEdges.length * 2;
				int[] newEdgesArray = new int[newEdgeArrayLength];
				System.arraycopy(tmpEdges, 0, newEdgesArray, 0, tmpEdges.length);
				edges[sourceId/2] = newEdgesArray;
				tmpEdges = newEdgesArray;
			}
			tmpEdges[sourceIdNumEdges] = destinationId;
			numEvenVerticesEdgeIndexes[sourceId/2] = sourceIdNumEdges + 1;
		}

//		Set<Integer> edgesWithoutDuplicates = new HashSet<Integer>();
		BufferedWriter[] bufferedWriters = new BufferedWriter[numberOfOutputFiles];
		for (int i = 0; i < numberOfOutputFiles; ++i) {
			bufferedWriters[i] = PartitionerUtils.getBufferedWriter(
				outputDirectory + "/" + outputFilePrefix + "_" + i);
		}
// 		BufferedWriter bufferedWriter = PartitionerUtils.getBufferedWriter(
//			outputDirectory + "/" + outputFilePrefix);
		BufferedWriter tmpBufferedWriter = null;
		int edgeIndex;
		int[] tmpNeighbors;
		for (int i = 0; i < numVertices; ++i) {
			if ((i % 5000000) == 0) {
				System.out.println("output " + i + "th vertex.");
			} 
			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append("" + i);
			if ((i % 2) == 0) {
				edgeIndex = numEvenVerticesEdgeIndexes[i/2];
				tmpNeighbors = edges[i/2];
				for (int j = 0; j < edgeIndex; ++j) {
					stringBuilder.append(" " + tmpNeighbors[j]);
//
//					edgesWithoutDuplicates.add(tmpNeighbors[j]);
				}
//				for (int neighborId : edgesWithoutDuplicates) {
//					stringBuilder.append(" " + neighborId);
//				}
//				edgesWithoutDuplicates.clear();
			}
			stringBuilder.append("\n");
			tmpBufferedWriter = bufferedWriters[i % numberOfOutputFiles];
			tmpBufferedWriter.write(stringBuilder.toString());
		}
		for (BufferedWriter bf : bufferedWriters) {
			bf.close();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("numberOfVertices: " + numVertices + " numEdges: " + numEdges);
		System.out.println("Total Time: " + (endTime - beginningTime));
	}
	
	private static CommandLine parseAndAssertCommandLines(String[] args) {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("outputdirectory", "od", true, "output directory");
		options.addOption("outputfile", "ofp", true, "output file prefix");
		options.addOption("numoutputfiles", "nof", true, "num output files");
		options.addOption("numberofvertices", "nv", true, "number of vertices");
		options.addOption("numberofedges", "ne", true, "number of edges");
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
