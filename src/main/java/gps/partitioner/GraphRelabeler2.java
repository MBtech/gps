package gps.partitioner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

public class GraphRelabeler2 {

	public static void main(String[] args) throws IOException {

		long startTime = System.currentTimeMillis();
		BufferedReader bufferedReader = new BufferedReader(new FileReader(args[0]), 10000000);
		String outputFileName = args[1] + "/" + args[2];
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFileName));
		String line;
		int source = -1;
		int destination = -1;
		int counter = 0;
		String[] split = null;
		int numNodes = -1;
		if (args.length < 4) {
			numNodes = countNumNodes(bufferedReader);
		} else {
			numNodes = Integer.parseInt(args[3]);
		}
		int[] edgeCount = new int[numNodes + 1];
		long previousTime = System.currentTimeMillis();
		int numEdges = 0;
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			split = line.split("\\s+");
			if (counter % 1000000 == 0) {
				long currentTime = System.currentTimeMillis();
				System.out.println("EdgeCounter " + counter + " lines. Time: "
					+ (currentTime - previousTime));
				previousTime = System.currentTimeMillis();
			}
			try {
				numEdges++;
				edgeCount[Integer.parseInt(split[0])]++;
				edgeCount[Integer.parseInt(split[1])]++;
			} catch (NumberFormatException e) {
			}
		}

		System.out.println("numEdges: " + numEdges);
		bufferedReader.close();

		bufferedReader = new BufferedReader(new FileReader(args[0]), 10000000);
		int[][] graph2 = new int[numNodes + 1][];
		for (int i = 0; i < numNodes + 1; ++i) {
			graph2[i] = new int[edgeCount[i]];
		}
		int[] pointers = new int[numNodes + 1];
		// ArrayList<Integer>[] graph = (ArrayList<Integer>[]) new ArrayList[numNodes + 1];
		// for (int i = 0; i < numNodes + 1; ++i) {
		// graph[i] = new ArrayList<Integer>(edgeCount[i]);
		// }
		numEdges = 0;
		previousTime = System.currentTimeMillis();
		Runtime r = Runtime.getRuntime();
		System.out.println("starting...");
		counter = 0;
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			split = line.split("\\s+");
			if (counter % 1000000 == 0) {
				r.gc();
				long currentTime = System.currentTimeMillis();
				System.out.println("Parsed " + counter + " lines. Time: "
					+ (currentTime - previousTime));
				previousTime = System.currentTimeMillis();
			}
			try {
				numEdges++;
				source = Integer.parseInt(split[0]);
				destination = Integer.parseInt(split[1]);
				graph2[source][pointers[source]++] = destination;
				graph2[destination][pointers[destination]++] = source;
				// graph[source].add(destination);
				// graph[destination].add(source);
			} catch (NumberFormatException e) {
			}
		}
		bufferedWriter.write(numNodes + " " + numEdges + "\n");
		String edges = "";
		for (int i = 1; i < numNodes; ++i) {
			if (i % 300000 == 0) {
				System.out.println("Writing " + i + " lines");
			}
			edges = "";

			HashSet foo = new HashSet();
			for (int k = 0; k < pointers[i]; ++k) {
				foo.add(new Integer(graph2[i][k]));
			}
			Iterator iterator = foo.iterator();
			while (iterator.hasNext()) {
				edges += " " + (Integer) iterator.next();
			}
			// for (int j : foo) {
			// edges += " " + j;
			// }
			bufferedWriter.write(edges.trim() + "\n");
		}
		bufferedWriter.close();
		System.out.println("numNodes: " + numNodes);
		System.out.println("numEdges: " + numEdges);
		// List<Integer> list = new ArrayList<Integer>(nodeIds);
		// Collections.sort(list);
		// System.out.println("first: " + list.get(0));
		// System.out.println("last: " + list.get(list.size() - 1));
		long endTime = System.currentTimeMillis();
		System.out.println("TimeTaken: " + (endTime - startTime));
	}

	private static int countNumNodes(BufferedReader bufferedReader) throws IOException {
		String line = null;
		String[] split = null;
		int counter = 0;
		HashSet nodeIds = new HashSet();
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			split = line.split("\\s+");
			if (counter % 1000000 == 0) {
				System.out.println("Parsed " + counter + " lines in countNumNodes");
			}
			try {
				nodeIds.add(new Integer(Integer.parseInt(split[0])));
				nodeIds.add(new Integer(Integer.parseInt(split[1])));
			} catch (NumberFormatException e) {
			}
		}
		System.out.println("numNodes: " + nodeIds.size());
		return nodeIds.size();
	}
}
