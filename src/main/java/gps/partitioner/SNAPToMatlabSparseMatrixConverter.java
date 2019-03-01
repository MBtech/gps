package gps.partitioner;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class SNAPToMatlabSparseMatrixConverter {

	public static void main(String args[]) throws IOException {
		int counter;
		Map<Integer, HashSet<Integer>> graph = PartitionerUtils.readSnapFileIntoGraphAndMakeUndirected(args[0]);
		String outputFile = args[0].substring(0, args[0].indexOf(".txt")) + ".mtl";
		outputMatlabFormat(outputFile, graph);
	}

	public static void outputMatlabFormat(String outputFile, Map<Integer, HashSet<Integer>> graph)
		throws IOException {
		int counter;
//		System.out.println("inputFile: " + inputFile + " outputFile: " + outputFile);
		BufferedWriter bufferedWriter = PartitionerUtils.getBufferedWriter(outputFile);
		List<Integer> keySet = new ArrayList<Integer>(graph.keySet());
		Collections.sort(keySet);
		counter = 0;
		for (int source : keySet) {
			for (int destination : graph.get(source)) {
				counter++;
				bufferedWriter.write(source + " " + destination + " 1\n");				
			}
		}
		System.out.println("numEdges: " + counter);
		bufferedWriter.close();
	}
}
