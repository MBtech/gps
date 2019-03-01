package gps.partitioner;

import gps.node.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PartitioningTester {

	public static void main(String[] args) throws IOException {
		testSplitMethod();

		List<String> hadoopConfFiles = new ArrayList<String>();
		hadoopConfFiles.add("/Users/semihsalihoglu/projects/hadoop-0.20.2/conf/core-site.xml");
		hadoopConfFiles.add("/Users/semihsalihoglu/projects/hadoop-0.20.2/conf/mapred-site.xml");
		FileSystem fileSystem = Utils.getFileSystem(hadoopConfFiles);
		Path path = new Path("/tmp/test_partitioning.txt");
		FileStatus fileStatus = fileSystem.getFileStatus(path);
		System.out.println("blockSize: " + fileStatus.getBlockSize());
		System.out.println("len: " + fileStatus.getLen());

		long startOffset = 0;
		long endOffset = 0;

		parseInputSplit(fileSystem, path, startOffset, endOffset);
	}

	private static void parseInputSplit(FileSystem fileSystem, Path path, long startOffset,
		long endOffset) throws IOException {
		InputStreamReader inputStreamReader = new InputStreamReader(fileSystem.open(path));
		int bufferLength = 3;
		char[] tmpCharArray = new char[bufferLength];
		long timeBeforeSkip = System.currentTimeMillis();
//		inputStreamReader.skip(startOffset-1);
		System.out.println("time to skip " + startOffset + "  characters: "
			+ (System.currentTimeMillis() - timeBeforeSkip));
//		startOffset += findIndexOfFirstNewLineChar(inputStreamReader) + 1;
		long currentOffset = startOffset;
		String[] split = null;
		int tmpCharArrayOffset = 0;
		String newLineRegex = "\n";
		String tmpStr;
		List<String> foundLines = new ArrayList<String>();
		while (true) {
			System.out.println("Starting a new iteration...");
			System.out.println("tmpCharArrayOffset: " + tmpCharArrayOffset
				+ " tmpCharArrayLength: " + tmpCharArray.length);
			System.out.println("currentOffset: " + currentOffset + " startOffset: " + startOffset
				+ " endOffset: " + endOffset);
			int maxReadableLength = Math.min((int) (endOffset - currentOffset + 1),
				tmpCharArray.length - tmpCharArrayOffset);
			System.out.println("maxReadableLength: " + maxReadableLength);
			int lengthOfActualRead = inputStreamReader.read(tmpCharArray, tmpCharArrayOffset,
				maxReadableLength);
			System.out.println("lengthOfActualRead: " + lengthOfActualRead);
			if (lengthOfActualRead == -1) {
				break;
			}
			currentOffset += lengthOfActualRead;
			tmpCharArrayOffset += lengthOfActualRead;
			if (currentOffset > endOffset) {
				break;
			} else {
				assert tmpCharArrayOffset == tmpCharArray.length;
			}
			tmpStr = new String(tmpCharArray);
			System.out.println("tmpStr: " + tmpStr);
			if (!tmpStr.contains(newLineRegex)) {
				System.out.println("No new line in str. Enlarging buffer size.");
				bufferLength = tmpCharArray.length * 2;
				System.out.println("oldBufferLength: " + tmpCharArray.length
					+ " newBufferLength: " + bufferLength);
				char[] tmp = new char[bufferLength];
				System.arraycopy(tmpCharArray, 0, tmp, 0, tmpCharArray.length);
				tmpCharArray = tmp;
				continue;
			}
			split = tmpStr.split(newLineRegex);
			boolean lastCharIsNewLine = tmpStr.charAt(tmpStr.length() - 1) == '\n';
			int lastSplitIndex = lastCharIsNewLine ? split.length
				: split.length - 1;
			System.out.println("Start of outputting lines...");
			for (int i = 0; i < lastSplitIndex; ++i) {
				foundLines.add(split[i]);
				System.out.println(split[i]);
			}
			System.out.println("End of outputting lines...");
			char[] lastLine = lastCharIsNewLine ? new char[0] : split[split.length - 1].toCharArray();
			System.out.println("lastLine: " + new String(lastLine));
			System.arraycopy(lastLine, 0, tmpCharArray, 0, lastLine.length);
			tmpCharArrayOffset = lastLine.length;
			System.out.println("tmpCharArrayOffset: " + tmpCharArrayOffset);
		}
		// Parse leftover string.
		System.out.println("Parsing leftover string...");
		tmpStr = new String(tmpCharArray, 0, tmpCharArrayOffset);
		tmpStr += findLastLine(inputStreamReader);
		System.out.println("Left over string: " + tmpStr);
		split = tmpStr.split(newLineRegex);
		System.out.println("Start of outputting lines...");
		for (int i = 0; i < split.length; ++i) {
			foundLines.add(split[i]);
			System.out.println(split[i]);
		}
		split = tmpStr.split(newLineRegex);

		System.out.println("Start of final lines...");
		for (String foundLine : foundLines) {
			System.out.println(foundLine);
		}
		System.out.println("End of final lines...");
	}

	private static String findLastLine(InputStreamReader inputStreamReader) throws IOException {
		String retVal = "";
		char nextChar = (char) inputStreamReader.read();
		while (!('\n' == nextChar)) {
			retVal += nextChar;
			nextChar = (char) inputStreamReader.read();
		}
		System.out.println("found last line: " + retVal);
		return retVal;
	}

	private static long findIndexOfFirstNewLineChar(InputStreamReader inputStreamReader)
		throws IOException {
		boolean foundNewLine = false;
		int indexOfNewLine = 0;
		while (!('\n' == inputStreamReader.read())) {
			indexOfNewLine++;
		}
		System.out.println("Index of first line: " + indexOfNewLine);
		return indexOfNewLine;
	}

	private static void testSplitMethod() {
		String foo = "foo";
		String[] split2 = foo.split(":");
		System.out.println("split2.length: " + split2.length);
		for (String bar : split2) {
			System.out.println(bar);
		}
		System.out.println("Done..");
	}
	
	public static FileSystem getFileSystem() throws IOException {
		Configuration configuration = new Configuration();
		configuration.addResource(
			"/Users/semihsalihoglu/projects/hadoop-0.20.2/conf/core-site.xml");
		configuration.addResource(
			"/Users/semihsalihoglu/projects/hadoop-0.20.2/conf/mapred-site.xml");
		return FileSystem.get(configuration);
	}
}
