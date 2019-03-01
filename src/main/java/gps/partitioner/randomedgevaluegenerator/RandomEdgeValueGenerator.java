package gps.partitioner.randomedgevaluegenerator;

import gps.node.Utils;
import gps.partitioner.PartitionerUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class RandomEdgeValueGenerator {
	public static void main(String[] args) throws IOException {
		long beginningTime = System.currentTimeMillis();
		CommandLine commandLine = parseAndAssertCommandLines(args);
		String inputDirectory = commandLine.getOptionValue("id");
		String outputDirectory = commandLine.getOptionValue("od");
		String inputFile = commandLine.getOptionValue("if");
		String outputFile = outputDirectory + "/" + inputFile + ".withrandomedgevalues";
		BufferedReader bufferedReader = PartitionerUtils.getBufferedReader(inputDirectory + "/" + inputFile);
		BufferedWriter bufferedWriter = PartitionerUtils.getBufferedWriter(outputFile);
		String line = null;
		String[] split = null;
		int counter = 0;
		long previousTime = System.currentTimeMillis();
		Random random = new Random(1);
		// Ignore the first line 39459926 lines.
		while ((line = bufferedReader.readLine()) != null) {
			counter++;
			if (counter % 1000000 == 0) {
				long currentTime = System.currentTimeMillis();
				System.out.println("Parsed " + counter + " lines. Time: "
					+ (currentTime - previousTime));
				previousTime = System.currentTimeMillis();
			}
			split = line.split("\\s+");
			bufferedWriter.write(split[0]);
			for (int i = 1; i < split.length; ++i) {
				bufferedWriter.write(" ");
				bufferedWriter.write(split[i]);
				bufferedWriter.write(" ");
				bufferedWriter.write("" + (random.nextInt(25) + 1));
			}
			bufferedWriter.write("\n");
		}
		bufferedReader.close();
		bufferedWriter.close();
		System.out.println("Done. TotalTime: " + (System.currentTimeMillis() - beginningTime));
	}
	
	private static CommandLine parseAndAssertCommandLines(String[] args) {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("inputdirectory", "id", true, "input directory");
		options.addOption("outputdirectory", "od", true, "output directory");
		options.addOption("inputfile", "if", true, "input file");
		try {
			CommandLine line = parser.parse(options, args);
			return line;
		} catch (ParseException e) {
			System.err.println("Unexpected exception:" + e.getMessage());
			System.exit(-1);
			return null;
		}
	}
}
