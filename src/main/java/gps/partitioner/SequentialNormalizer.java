package gps.partitioner;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SequentialNormalizer {

	public static void main(String[] args) throws FileNotFoundException, IOException {
		String inputFile = args[0];
		String outputFile = args[1];
		Map<Integer, Map<Integer, Integer>> regularGraph =
			PartitionerUtils.readSnapFileIntoGraphAndMakeUndirectedAndWeighted(inputFile);
		List<Integer> sortedKeySet = new ArrayList<Integer>(regularGraph.keySet());
		Collections.sort(sortedKeySet);
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFile));
		for (int vertexId : sortedKeySet) {
			bufferedWriter.write("" + vertexId);
			for (int neighborId : regularGraph.get(vertexId).keySet()) {
				bufferedWriter.write(" " + neighborId);
			}
			bufferedWriter.write("\n");
		}
		bufferedWriter.close();
	}
}
