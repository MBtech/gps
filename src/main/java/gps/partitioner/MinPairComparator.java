package gps.partitioner;

import gps.node.Pair;

import java.util.Comparator;
import java.util.PriorityQueue;

public class MinPairComparator implements Comparator<Pair<String, Integer>> {

	@Override
	public int compare(Pair<String, Integer> firstPair, Pair<String, Integer> secondPair) {
		if (firstPair.snd > secondPair.snd) {
			return -1;
		}
		if (firstPair.snd < secondPair.snd) {
			return 1;
		}
		return 0;
	}

	public static void main(String[] args) {
		Comparator<Pair<String, Integer>> minPairComparator = new MinPairComparator();
		PriorityQueue<Pair<String, Integer>> highDegreeVertices =
			new PriorityQueue<Pair<String,Integer>>(5, new MaxPairComparator());
		highDegreeVertices.add(Pair.of("bar1", 5));
		highDegreeVertices.add(Pair.of("bar2", 2));
		highDegreeVertices.add(Pair.of("bar3", 9));
		highDegreeVertices.add(Pair.of("bar4", 3));
		highDegreeVertices.add(Pair.of("bar5", 1));
		System.out.println(highDegreeVertices.poll().snd);
		System.out.println(highDegreeVertices.poll().snd);
		System.out.println(highDegreeVertices.poll().snd);
		System.out.println(highDegreeVertices.poll().snd);
		System.out.println(highDegreeVertices.poll().snd);
	}
}