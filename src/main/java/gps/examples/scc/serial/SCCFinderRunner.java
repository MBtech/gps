package gps.examples.scc.serial;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Stack;

public class SCCFinderRunner {

	public HashMap<Integer, Node> regularNodeHashMap = new HashMap<Integer, Node>();
	public HashMap<Integer, Node> transposeNodeHashMap = new HashMap<Integer, Node>();
	public Stack<Node> regularNodes = new Stack<Node>();
	public LinearSCCFinder sccFinder = null;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		long beginningTime = System.currentTimeMillis();
		if (args.length != 2) {
			System.out.println("You have to have 2 arguments! "
				+ "The first one is the input file " + "and the second one is the output file.");
		}
		SCCFinderRunner sccFinderRunner = new SCCFinderRunner();
		sccFinderRunner.parseInputFile(args[0]);
		int[] sccsInDescendingOrder = sccFinderRunner.findSCCs();
		PrintWriter out = new PrintWriter(new FileWriter(args[1]));
		out.print(sccsInDescendingOrder[0]);
		for (int i = 1; i < 5; ++i) {
			if (i >= sccsInDescendingOrder.length) {
				out.print("\t" + 0);
			} else {
				out.print("\t" + sccsInDescendingOrder[i]);
			}
		}
		out.close();
		long endingTime = System.currentTimeMillis();
		double runTimeInSeconds = ((double) (endingTime - beginningTime)) / 1000.0;
		System.out.println("Run time: " + runTimeInSeconds);
	}

	private void parseInputFile(String inputfile) throws IOException {
		FileInputStream fstream = new FileInputStream(inputfile);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		// Skipping the first two lines
		br.readLine();
		br.readLine();
		int numEdgesParsed = 0;
		while ((strLine = br.readLine()) != null) {
			numEdgesParsed++;
			if ((numEdgesParsed % 1000000) == 0) {
				System.out.println("parsed " + numEdgesParsed + " edges...");
			}
			String[] split = strLine.split("\t");
			addEdge(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
		}
	}
	
	public int[] findSCCs() {
		sccFinder = new LinearSCCFinder(transposeNodeHashMap, regularNodes);
		return sccFinder.findSCCS();
	}
	
	public void addEdge(int sourceId, int destId) {
		addEdgeToNodeHashMapAndStack(sourceId, destId, regularNodeHashMap, regularNodes);
		addEdgeToNodeHashMapAndStack(destId, sourceId, transposeNodeHashMap, null);
	}

	private static void addEdgeToNodeHashMapAndStack(int sourceId, int destId,
		HashMap<Integer, Node> nodeHashMap, Stack<Node> regularNodes) {
		Node sourceNode;
		if (nodeHashMap.containsKey(sourceId)) {
			sourceNode = nodeHashMap.get(sourceId);
		} else {
			sourceNode = new Node(sourceId);
			nodeHashMap.put(sourceId, sourceNode);
			if (regularNodes != null) {
				regularNodes.push(sourceNode);
			}
		}
		Node destNode;
		if (nodeHashMap.containsKey(destId)) {
			destNode = nodeHashMap.get(destId);
		} else {
			destNode = new Node(destId);
			nodeHashMap.put(destId, destNode);
			if (regularNodes != null) {
				regularNodes.push(destNode);
			}
		}
		sourceNode.children.add(destNode);
	}

}
