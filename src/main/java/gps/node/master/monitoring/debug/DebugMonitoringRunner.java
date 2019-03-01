package gps.node.master.monitoring.debug;

import gps.node.MachineStats;
import gps.node.MachineStats.StatName;
import gps.node.Utils;
import gps.node.master.monitoring.Server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class DebugMonitoringRunner {

	public static final int DEFAULT_MONITORING_PORT = 9999;

	private static final String MACHINE_STATS_FILES_PATH_SHORT_OPT_NAME = "msfp";
	private static final String MACHINE_STATS_FILES_PATH_OPT_NAME = "machinestatsfilespath";
	private static final String PORT_OPT_NAME = "port";
	private static final String PORT_SHORT_OPT_NAME = "p";

	private static Logger logger = Logger.getLogger(DebugMonitoringRunner.class);

	public static void main(String[] args) throws IOException {
		CommandLine line = parseAndAssertCommandLines(args);
		FileSystem fileSystem = Utils.getFileSystem(Utils.getHadoopConfFiles(line));
		logger.info(MACHINE_STATS_FILES_PATH_OPT_NAME + ":" + 
			line.getOptionValue(MACHINE_STATS_FILES_PATH_OPT_NAME));
		String[] split = line.getOptionValue(MACHINE_STATS_FILES_PATH_OPT_NAME).split(" ");
		Map<String, MachineStats> machineStatsMap = new HashMap<String, MachineStats>();
		int port = line.hasOption(PORT_OPT_NAME) ?
			Integer.parseInt(line.getOptionValue(PORT_OPT_NAME)) : DEFAULT_MONITORING_PORT;
		for (String machineStatFile : split) {
			FileStatus[] globStatus = fileSystem.globStatus(
			new Path(machineStatFile));
			logger.info("number of machine stats files: " + globStatus.length);
			for (FileStatus fileStatus : globStatus) {
				logger.info("FileStatus: " + fileStatus.getPath().getName());
				Path path = fileStatus.getPath();
				machineStatsMap.put(getExecutionName(path), getMachineStats(fileSystem, path));
			}
		}
		new Server(new DebugServerHandler(machineStatsMap), port).start();
	}

	private static MachineStats getMachineStats(FileSystem fileSystem, Path path)
		throws IOException {
		logger.info("Beginning of parsing machine stats. path: " + path.getName());
		BufferedReader bufferedReader =
			new BufferedReader(new InputStreamReader(fileSystem.open(path)));
		MachineStats machineStats = new MachineStats();
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			String[] split = line.split(" ");
			if (!isDefaultStat(split[0])) {
				machineStats.addCustomStatKey(getCustomStatName(split[0]));
			}
			if (Boolean.parseBoolean(split[2])) {
				machineStats.putDoubleStat(split[0], Double.parseDouble(split[1]));
			} else {
				machineStats.putStringStat(split[0], split[1]);			
			}
		}
		bufferedReader.close();
		logger.info("End of parsing machine stats. path: " + path.getName());
		return machineStats;
	}

	private static String getCustomStatName(String customStatNameInMachineConfigFormat) {
		// Inside machine config format a stat could be in the form of int-statName or
		// int-statName-int. We check if either is the case and return only statName
		String[] splits = customStatNameInMachineConfigFormat.split("-");
		int startIndex = 0;
		if (splits.length > 1) {
			try {
				Integer.parseInt(splits[0]);
				startIndex = splits[0].length() + 1;
			} catch(NumberFormatException e) {
				return customStatNameInMachineConfigFormat;
			}
			try {
				Integer.parseInt(splits[splits.length - 1]);
				return customStatNameInMachineConfigFormat.substring(startIndex,
					customStatNameInMachineConfigFormat.length() - (splits[splits.length - 1].length() + 1));
			} catch(NumberFormatException e) {
				return customStatNameInMachineConfigFormat.substring(startIndex);
			}
		}
		return null;
	}

	private static boolean isDefaultStat(String statKey) {
		String[] splitByDash = statKey.split("-");
		for (String stringSection : splitByDash) {
			if (StatName.isDefaultStatName(stringSection)) {
				
				return true;
			}
		}
		return false;
	}

	private static String getExecutionName(Path path) {
		return path.getName().replace("-machine-stats", "").toLowerCase();
	}

	private static CommandLine parseAndAssertCommandLines(String[] args) {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption(MACHINE_STATS_FILES_PATH_SHORT_OPT_NAME,
			MACHINE_STATS_FILES_PATH_OPT_NAME, true, "where to get the machine stats files");
		options.addOption(Utils.HADOOP_CONF_FILES_OPT_NAME,
			Utils.HADOOP_CONF_FILES_SHORT_OPT_NAME, true, "hadoop conf files");
		options.addOption(PORT_OPT_NAME, PORT_SHORT_OPT_NAME, true, "port");
		try {
			return parser.parse(options, args);
		} catch (ParseException e) {
			logger.error("Unexpected exception:" + e.getMessage());
			System.exit(-1);
			return null;
		}
	}
}
