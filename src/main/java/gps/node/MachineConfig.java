package gps.node;



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

/**
 * In memory representation of the host-name:port for each machine.
 * 
 * @author semihsalihoglu
 */
public class MachineConfig {

	private Map<Integer, Pair<String, Integer>> machineConfigurations;
	private Set<Integer> workerIds;
	private int maxmachineId;

	public MachineConfig() {
		this.machineConfigurations = new HashMap<Integer, Pair<String, Integer>>();
		this.workerIds = new HashSet<Integer>();
		this.maxmachineId = Byte.MIN_VALUE;
	}

	/**
	 * Loads the machine configurations from a file.
	 * 
	 * @param fileName name of the file to read from
	 * @throws IOException
	 * @return returns itself for chaining
	 */
	public MachineConfig load(FileSystem fileSystem, String fileName) throws IOException {
		BufferedReader bufferedReader =
			new BufferedReader(new InputStreamReader(fileSystem.open(new Path(fileName))));
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			Iterator<String> iterator =
				Splitter.on(CharMatcher.WHITESPACE).trimResults().omitEmptyStrings().split(line)
					.iterator();
			addMachine(Integer.parseInt(iterator.next()), iterator.next(),
				Integer.parseInt(iterator.next()));
		}
		return this;
	}

	public synchronized List<Integer> getWorkerIds() {
		return new ArrayList<Integer>(workerIds);
	}

	public synchronized Set<Integer> getAllMachineIds() {
		return new HashSet<Integer>(machineConfigurations.keySet());
	}

	public synchronized Pair<String, Integer> getHostPortPair(int machineId) {
		return machineConfigurations.get(machineId);
	}

	public int getMaxmachineId() {
		return maxmachineId;
	}

	public MachineConfig addMachine(int machineId, String host, int port) {
		machineConfigurations.put(machineId, Pair.of(host, port));
		if (machineId > maxmachineId) {
			maxmachineId = machineId;
		}
		if (Utils.MASTER_ID != machineId) {
			workerIds.add(machineId);
		}
		return this;
	}
}
