package gps.partitioner.highestdegreedomainsdumper;

import gps.node.Pair;
import gps.partitioner.MaxPairComparator;
import gps.partitioner.MinPairComparator;
import gps.partitioner.PartitionerUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class HighestDegreeDomainsDumper {

	public static void main(String[] args) throws IOException {
		System.out.println("new version 2");
		BufferedReader bufferedReader = PartitionerUtils.getBufferedReader(args[0]);
		int numVerticesToDump = Integer.parseInt(args[1]);
		Comparator<Pair<String, Integer>> maxPairComparator = new MaxPairComparator();
		PriorityQueue<Pair<String, Integer>> highNumVerticesDomains =
			new PriorityQueue<Pair<String,Integer>>(numVerticesToDump, maxPairComparator);
		PriorityQueue<Pair<String, Integer>> highNumOutgoingEdgesDomains =
			new PriorityQueue<Pair<String,Integer>>(numVerticesToDump, maxPairComparator);
		PriorityQueue<Pair<String, Integer>> highNumIncomingEdgesDomains =
			new PriorityQueue<Pair<String,Integer>>(numVerticesToDump, maxPairComparator);
		String line = null;
		int counter = 0;
		String[] split = null;
		long previousTime = System.currentTimeMillis();
		int numVertices = -1;
		int numOutgoingEdges = -1;
		int numIncomingEdges = -1;
		String domain;
		// Ignore the first line.
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			split = line.split("\\s+");
			domain = split[0];
			numVertices = Integer.parseInt(split[1]);
			numOutgoingEdges = Integer.parseInt(split[2]);
			numIncomingEdges = Integer.parseInt(split[3]);
			if (counter % 1000000 == 0) {
				long currentTime = System.currentTimeMillis();
				System.out.println("Parsed " + counter + " lines. Time: "
					+ (currentTime - previousTime));
				previousTime = System.currentTimeMillis();
			}
			if (counter % 10000000 == 0) {
				dumpHighDegreeDomains(highNumVerticesDomains, "numVertices");
				dumpHighDegreeDomains(highNumOutgoingEdgesDomains, "numOutgoingEdges");
				dumpHighDegreeDomains(highNumIncomingEdgesDomains, "numIncomingEdges");
			}
			addDomainToPriorityQueue(numVerticesToDump, highNumVerticesDomains, numVertices, domain);
			addDomainToPriorityQueue(numVerticesToDump, highNumOutgoingEdgesDomains, numOutgoingEdges, domain);
			addDomainToPriorityQueue(numVerticesToDump, highNumIncomingEdgesDomains, numIncomingEdges, domain);
		}
		bufferedReader.close();
		dumpHighDegreeDomains(highNumVerticesDomains, "numVertices");
		dumpHighDegreeDomains(highNumOutgoingEdgesDomains, "numOutgoingEdges");
		dumpHighDegreeDomains(highNumIncomingEdgesDomains, "numIncomingEdges");
	}

	private static void addDomainToPriorityQueue(int numDomainsToDump,
		PriorityQueue<Pair<String, Integer>> priorityQueue, int domainValue, String domain) {
		if (priorityQueue.size() < numDomainsToDump) {
			priorityQueue.add(new Pair<String, Integer>(domain,
				domainValue));
		} else if (priorityQueue.peek().snd < domainValue) {
			priorityQueue.poll();
			priorityQueue.add(new Pair<String, Integer>(domain,
				domainValue));
		}
	}

	private static void dumpHighDegreeDomains(
		PriorityQueue<Pair<String, Integer>> highDegreeVertices, String statName) {
		List<Pair<String, Integer>> sortedVertices = new ArrayList<Pair<String,Integer>>(
			highDegreeVertices);
		Collections.sort(sortedVertices, new MinPairComparator());
		System.out.println("Dumping highestDegreeVertices...");
		for (Pair<String, Integer> vertexIdDegreePair : sortedVertices) {
			System.out.println("domain: " + vertexIdDegreePair.fst
				+ " " + statName + ": " + vertexIdDegreePair.snd);
		}
	}
}
