package gps.partitioner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

public class UrlsWithIdsGenerator {

	private static Logger logger = Logger.getLogger(UrlsWithIdsGenerator.class);

	private static CommandLine parseAndAssertCommandLines(String[] args) {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("if", "inputfile", true, "name of the input file");
		options.addOption("od", "outputdirectory", true, "name of the output directory");
		options.addOption("gn", "graphname", true, "name of the graph");
		options.addOption("gzip", "gzip", true, "gzip file or not");
		try {
			CommandLine line = parser.parse(options, args);
			if (!line.hasOption("if") || !line.hasOption("od") || !line.hasOption("gn")) {
				logger.error("You must specify all the options: if, od, gn");
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
			+ "-urls-with-ids.txt");
		String line = null;
		long timeBefore = System.currentTimeMillis();
		long timeAfter = -1;
		int counter = -1;
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			bufferedWriter.write("-2 " + counter + " " + line + "\n");
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
