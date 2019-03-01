package gps.examples.scc.serial;

import gps.examples.scc.serial.Node.Color;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

public class LinearSCCFinder {

	private final HashMap<Integer, Node> transposeNodeHashMap;
	private final Stack<Node> regularNodes;
	private static int numVerticesVisited = 1;
	public  static List<Set<Node>> sccs = new ArrayList<Set<Node>>();

	public LinearSCCFinder(HashMap<Integer, Node> transposeNodeHashMap, Stack<Node> regularNodes) {
		this.transposeNodeHashMap = transposeNodeHashMap;
		this.regularNodes = regularNodes;
	}

	public int[] findSCCS() {
		Stack<Node> finishingTimes = new Stack<Node>();
		while (!regularNodes.empty()) {
			Node node = regularNodes.pop();
			if (node.dfsSearchColor == Color.WHITE) {
				iterativeDfs(node, finishingTimes, null);
				// runDfs(node, finishingTimes, null);
			} else if (node.dfsSearchColor == Color.BLACK) {
				continue;
			} else {
				System.out.println("FOUND A GRAY NODE IN THE SECOND DFS."
					+ "SHOULD NOT HAVE HAPPENED!! id: " + node.id);
			}
			// runDfs(regularNodeHashMap.get(nodeIdsForInitialDFS.iterator().next()),
			// finishingTimes, null);
		}

		List<Integer> sccSizes = new LinkedList<Integer>();
		while (!finishingTimes.isEmpty()) {
			Node node = finishingTimes.pop();
			if (node.dfsSearchColor == Color.WHITE) {
				Set<Node> scc = new HashSet<Node>();
				iterativeDfs(node, null, scc);
				sccs.add(scc);
				// runDfs(node, null, scc);
				sccSizes.add(scc.size());
//				System.out.println("PRINTING NEW SCC. size: " + scc.size());
				int maxId = -1;
				for (Node dummyNode : scc) {
					if (dummyNode.id > maxId) {
						maxId = dummyNode.id;
					}
//					System.out.print(" " + dummyNode.id);
				}
//				System.out.println("\nmaxId: " + maxId + "\n");
			} else if (node.dfsSearchColor == Color.BLACK) {
				continue;
			} else {
				System.out.println("FOUND A GRAY NODE IN THE SECOND DFS."
					+ "SHOULD NOT HAVE HAPPENED!! id: " + node.id);
			}
		}
		Collections.sort(sccSizes);
		int[] retVal = new int[5];
		int totalSize = 0;
		for (int i = 0; i < 5; i++) {
			if (i >= sccSizes.size()) {
				retVal[i] = 0;
			} else {
				int size = sccSizes.get(sccSizes.size() - (i + 1));
				retVal[i] = size;
				System.out.print("" + size + "\t");
				totalSize += size;
			}
		}
		return retVal;
	}

	private void iterativeDfs(Node node, Stack<Node> finishingTimes, Set<Node> scc) {
		Stack<Node> stack = new Stack<Node>();
		stack.push(node);
		while (!stack.isEmpty()) {
			Node currentNode = stack.pop();
			if (currentNode.dfsSearchColor == Color.WHITE) {
				numVerticesVisited++;
				if ((numVerticesVisited % 100000) == 0) {
					System.out.println("visited " + numVerticesVisited + " vertices in dfs...");
				}
				currentNode.dfsSearchColor = Color.GRAY;
				if (scc != null) {
					scc.add(currentNode);
				}
				stack.push(currentNode);
			} else if (currentNode.dfsSearchColor == Color.GRAY) {
				currentNode.dfsSearchColor = Color.BLACK;
				if (finishingTimes != null) {
					finishingTimes.push(transposeNodeHashMap.get(currentNode.id));
				}
				continue;
			}
			for (Node childOfCurrentNode : currentNode.children) {
				if (childOfCurrentNode.dfsSearchColor == Color.WHITE) {
					stack.push(childOfCurrentNode);
				}
			}
		}
	}

	private void runDfs(Node node, Stack<Node> finishingTimes, Set<Node> scc) {
		if (node.dfsSearchColor == Color.WHITE) {
			node.dfsSearchColor = Color.GRAY;
			if (scc != null) {
				scc.add(node);
			}
			while (node.dfsSearchIndex < node.children.size()) {
				runDfs(node.children.get(node.dfsSearchIndex), finishingTimes, scc);
				node.dfsSearchIndex++;
			}
			node.dfsSearchColor = Color.BLACK;
			if (finishingTimes != null) {
				finishingTimes.push(transposeNodeHashMap.get(node.id));
			}
		} else {
			return;
		}
	}
}
