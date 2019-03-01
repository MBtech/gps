package gps.partitioner;

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

public class OutputVerifier {

	private static Logger logger = Logger.getLogger(OutputVerifier.class);

	public static final String FIRST_OUTPUT_FILE_PREFIX_OPT_NAME = "firstfileprefix";
	public static final String FIRST_OUTPUT_FILE_PREFIX_SHORT_OPT_NAME = "ffp";
	public static final String SECOND_OUTPUT_FILE_PREFIX_OPT_NAME = "secondfileprefix";
	public static final String SECOND_OUTPUT_FILE_PREFIX_SHORT_OPT_NAME = "sfp";

	public static void main(String[] args) throws IOException {
		CommandLine line = parseAndAssertCommandLines(args);
		if (!line.hasOption(FIRST_OUTPUT_FILE_PREFIX_OPT_NAME)
			|| !line.hasOption(SECOND_OUTPUT_FILE_PREFIX_OPT_NAME)) {
			logger.info("You have to give both the -ffp (first file prefix) and " +
				"-sfp (second file prefix) as arguments. Exiting.");
			System.exit(-1);
		}

		Map<Integer, Double> firstFileValues = getIdValueMap(
			line.getOptionValue(FIRST_OUTPUT_FILE_PREFIX_OPT_NAME));
		Map<Integer, Double> secondFileValues = getIdValueMap(
			line.getOptionValue(SECOND_OUTPUT_FILE_PREFIX_OPT_NAME));
		logger.info("FirstOutputSize: " + firstFileValues.size() + " SecondOutputSize: "
			+ secondFileValues.size());
		double threshold = Double.parseDouble("1.0E-13");
		logger.info("Threshold: " + threshold);
		int numAboveThresholdFound = 0;
		int numDifferentWithNonZeroDiffrenceFound = 0;
		int numDifferentFound = 0;
//		if (firstFileValues.equals(secondFileValues)) {
//			logger.info("SAME OUTPUTS!");
//		} else {
			logger.info("DIFFERENT OUTPUTS!");
			Double firstValue;
			Double secondValue;
			logger.info("first output size: " + firstFileValues.size() +
				" second output size: " + secondFileValues.size());
			if (firstFileValues.size() != secondFileValues.size()) {
				logger.info("Outputs are not the same size");
			}
			for (int vertexId : firstFileValues.keySet()) {
				firstValue = firstFileValues.get(vertexId);
				secondValue = secondFileValues.get(vertexId);
				if (firstValue == null) {
//					logger.info("First value is null. secondValue: " + secondValue);
				} else if (secondValue == null) {
//					logger.info("Second value is null. firstValue: " + firstValue);					
				} else if (Math.abs(firstValue - secondValue) > threshold) {
					numAboveThresholdFound++;
					logger.info("above threshold output for vertexId: " + vertexId + " is different: " 
						+ "first value: " + firstValue + " secondValue: " + secondValue
						+ " diff: " + Math.abs(firstValue - secondValue));
					numDifferentWithNonZeroDiffrenceFound++;
					numDifferentFound++;
				} else if (Math.abs(firstValue - secondValue) > 0) {
//					logger.info("output for vertexId: " + vertexId + " is different: " 
//						+ "first value: " + firstValue + " secondValue: " + secondValue
//						+ " diff: " + Math.abs(firstValue - secondValue));
					numDifferentWithNonZeroDiffrenceFound++;
					numDifferentFound++;
				} else if (firstValue != secondValue) {
					numDifferentFound++;
				}
			}
//		}
		logger.info("Num different found: " + numDifferentFound);
		logger.info("Num different with non zero difference found: " + numDifferentWithNonZeroDiffrenceFound);
		logger.info("Num above threshold found: " + numAboveThresholdFound);
	}

	private static Map<Integer, Double> getIdValueMap(String filePrefix) throws IOException {
		FileSystem fileSystem = FileSystem.get(new Configuration());
		FileStatus[] listStatus = fileSystem.globStatus(new Path(filePrefix + "-output-*-of-*"));
		Map<Integer, Double> retVal = new HashMap<Integer, Double>();
		for (FileStatus fileStatus : listStatus) {
			BufferedReader bufferedReader =
				new BufferedReader(new InputStreamReader(fileSystem.open(fileStatus.getPath())));
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				String[] split = line.split("\\s+");
				try {
					retVal.put(Integer.parseInt(split[0]), Double.parseDouble(split[1]));
				} catch (Exception e) {
					logger.error("Unexpected exception:" + e.getMessage());
					System.exit(-1);
				}
			}
		}
		return retVal;
	}

	private static CommandLine parseAndAssertCommandLines(String[] args) {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption(FIRST_OUTPUT_FILE_PREFIX_OPT_NAME,
			FIRST_OUTPUT_FILE_PREFIX_SHORT_OPT_NAME, true, "column number");
		options.addOption(SECOND_OUTPUT_FILE_PREFIX_OPT_NAME,
			SECOND_OUTPUT_FILE_PREFIX_SHORT_OPT_NAME, true, "column number");
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
