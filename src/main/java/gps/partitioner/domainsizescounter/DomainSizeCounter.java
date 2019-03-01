package gps.partitioner.domainsizescounter;

import gps.partitioner.PartitionerUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

public class DomainSizeCounter {
	private static Logger logger = Logger.getLogger(DomainSizeCounter.class);

	private static CommandLine parseAndAssertCommandLines(String[] args) {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("if", "inputfile", true, "name of the input file");
		options.addOption("od", "outputdirectory", true, "name of the output directory");
		options.addOption("gn", "graphname", true, "name of the graph");
		options.addOption("np", "numberofpartitions", true, "number of partitions");
		options.addOption("gzip", "gzip", true, "gzip file or not");
		try {
			CommandLine line = parser.parse(options, args);
			if (!line.hasOption("if") || !line.hasOption("od") || !line.hasOption("gn")
				|| !line.hasOption("np")) {
				logger.error("You must specify all the options: if, od, gn, np");
				System.exit(-1);
			}
			return line;
		} catch (ParseException e) {
			logger.error("Unexpected exception:" + e.getMessage());
			System.exit(-1);
			return null;
		}
	}

	public static void main(String[] args) throws IOException {
		CommandLine commandLine = parseAndAssertCommandLines(args);
		String urlsFileName = commandLine.getOptionValue("if");
		System.out.println("Reading urlsFile: " + urlsFileName);
		InputStreamReader in;
		if (commandLine.hasOption("gzip") &&
			Boolean.parseBoolean(commandLine.getOptionValue("gzip"))) {
			in = new InputStreamReader(
				new GZIPInputStream(new FileInputStream(urlsFileName)));			
		} else {
			in = new InputStreamReader(new FileInputStream(urlsFileName));
		}
		BufferedReader bufferedReader = new BufferedReader(in);
		BufferedWriter bufferedWriter = PartitionerUtils.getBufferedWriter(
			commandLine.getOptionValue("od") + "/" + commandLine.getOptionValue("gn")
			+ "-ids-mapped-by-domain");
		int numPartitions = Integer.parseInt(commandLine.getOptionValue("np"));
		int[] nextIdsToGive = new int[numPartitions];
		for (int i = 0; i < numPartitions; ++i) {
			nextIdsToGive[i] = i;
		}

		String line = null;
		String tmpStr = null;
		String tmpDomain = null;
		String lastDomain = "";
		int lastGivenIdIndex = -1;
		long timeBefore = System.currentTimeMillis();
		long timeAfter = -1;
//		String[] split;
		int counter = -1;
		while ((line = bufferedReader.readLine()) != null) {
//			split = line.split("\\s+");
//			counter = Integer.parseInt(split[0]);
//			tmpStr = split[1].replace("http://", "");
			counter++;
			tmpStr = line.replace("http://", "");
			tmpDomain = tmpStr.substring(0, tmpStr.indexOf("/"));
//			logger.info("tmpStr: " + tmpStr);
//			logger.info("tmpDomain: " + tmpDomain);
//			logger.info("lastDomain: " + lastDomain);
			if (!lastDomain.equals(tmpDomain)) {
				lastGivenIdIndex = (lastGivenIdIndex + 1) % numPartitions;
				lastDomain = tmpDomain;
			}
			bufferedWriter.write("-1 " + counter + " " + nextIdsToGive[lastGivenIdIndex] + "\n");
			nextIdsToGive[lastGivenIdIndex] += numPartitions;

//			counter++;
			if (counter % 1000000 == 0) {
				timeAfter = System.currentTimeMillis();
				System.out.println("Parsed " + counter + " lines. Time: "
					+ (timeAfter - timeBefore));
				timeBefore = timeAfter;
			}
		}
		bufferedReader.close();
		bufferedWriter.close();
	}
}

