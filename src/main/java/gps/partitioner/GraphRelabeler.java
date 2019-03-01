package gps.partitioner;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class GraphRelabeler {

	public static void main(String[] args) throws ParseException, IOException {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("if", "inputfile", true, "location of the machine configuration file");
		options.addOption("od", "outputdir", true, "location of the machine configuration file");
		options.addOption("of", "outputfile", true, "location of the machine configuration file");
		options.addOption("nn", "numNodes", true, "location of the machine configuration file");
		CommandLine commandLine = parser.parse(options, args);
		assert commandLine.hasOption("if");
		assert commandLine.hasOption("od");
		assert commandLine.hasOption("of");

		long startTime = System.currentTimeMillis();
		String inputFile = commandLine.getOptionValue("if");
		String outputFileName = PartitionerUtils.getOutputFileName(commandLine);
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFileName));
		int numNodes;
		if (!commandLine.hasOption("nn")) {
			numNodes = PartitionerUtils.countNumNodes(inputFile);
		} else {
			numNodes = Integer.parseInt(commandLine.getOptionValue("nn"));
		}
		Graph graphObj = PartitionerUtils.getGraph(inputFile, numNodes, true /* undirected */);
		int[][] graph = graphObj.graph;
		String edges = "";
		HashSet<Integer> cleanNumEdges = new HashSet<Integer>();
		for (int i = 1; i < numNodes; ++i) {
			System.out.println("Writing nodeId: " + i + " neighbors: " + graph[i].length);
			if (i % 300000 == 0) {
				System.out.println("Writing " + i + " lines");
			}
			edges = "";
			for (int k = 0; k < graph[i].length; ++k) {
				cleanNumEdges.add(graph[i][k]);
			}
			for (int j : cleanNumEdges) {
				edges += " " + j;
			}
			cleanNumEdges.clear();
			bufferedWriter.write(edges.trim() + "\n");
		}
		bufferedWriter.close();
		System.out.println("numNodes: " + numNodes);
		System.out.println("numEdges: " + graphObj.numEdges);
		long endTime = System.currentTimeMillis();
		System.out.println("TimeTaken: " + (endTime - startTime));
	}
}
