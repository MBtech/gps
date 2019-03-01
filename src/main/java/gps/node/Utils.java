package gps.node;

import gps.graph.Graph;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * A collection of utility methods abstracted to a Utils class to make testing easier.
 * 
 * @author semihsalihoglu
 */
public class Utils {

	private static Logger logger = Logger.getLogger(Utils.class);

	public static final String HADOOP_CONF_FILES_OPT_NAME = "hadoopconffiles";
	public static final String HADOOP_CONF_FILES_SHORT_OPT_NAME = "hcf";

	public static final int MASTER_ID = -1;
	public static final int ONE_SECOND_IN_MILLISECONDS = 1000;
	public static final String HDFS_VERTEX_SHUFFLES_DIRECTORY =
		"/user/semih/gps/runtime/vertex-shuffles";
	public static final String LOGGER_HEADER_SECOND_PART = " ************************";
	public static final String LOGGER_HEADER_FIRST_PART = "************************ ";

	public static final String COMMAND_LINE_STAT_PREFIX = "commandline";
	public static final String COMMAND_LINE_STAT_VALUE_SEPARATOR = "---";
	public static final String JVM_ARGS_PREFIX = "jvmargs";
	
	public static CharsetDecoder ISO_8859_1_DECODER;
	public static CharsetEncoder ISO_8859_1_ENCODER;
	static {
		Charset charset = Charset.forName("ISO-8859-1");
		ISO_8859_1_DECODER = charset.newDecoder();
		ISO_8859_1_ENCODER = charset.newEncoder();
	}

	public static FileSystem getFileSystem(List<String> hadoopConfFiles) throws IOException {
		Configuration configuration = new Configuration();
		logger.info("fs.hdfs.impl: " + configuration.get("fs.hdfs.impl"));
		logger.info("fs.default.name: " + configuration.get("fs.default.name"));
		for (String confFile : hadoopConfFiles) {
			logger.info("adding conf file: " + confFile);
			configuration.addResource(new Path(confFile));			
		}
		logger.info("fs.hdfs.impl: " + configuration.get("fs.hdfs.impl"));
		logger.info("fs.default.name: " + configuration.get("fs.default.name"));
		return FileSystem.get(configuration);
	}

	public static int[] getRandomPermutation(int permutationSize) {
		int[] permutation = new int[permutationSize];
		// insert integers 0..N-1
		for (int i = 0; i < permutationSize; i++)
			permutation[i] = i;
		// shuffle
		for (int i = 0; i < permutationSize; i++) {
			int r = (int) (Math.random() * (i + 1)); // int between 0 and i
			int swap = permutation[r];
			permutation[r] = permutation[i];
			permutation[i] = swap;
		}
		return permutation;
	}

	public static boolean isEven(int superstepNo) {
		return (superstepNo % 2) == 0;
	}

	public static void writeMachineStats(FileSystem fileSystem, MachineStats machineStats,
		String outputFile) throws IOException {
		BufferedWriter bw = getBufferedWriter(fileSystem, outputFile);
		Map<String, Double> machineDoubleStats = machineStats.getMachineDoubleStats();
		// Note: Below true indicates that the machinestat is a double value (and false is
		// a string value further below in the second for loop).
		for (String key : machineDoubleStats.keySet()) {
			bw.write(key + " " + machineDoubleStats.get(key) + " true\n");
		}
		Map<String, String> machineStringStats = machineStats.getMachineStringStats();
		for (String key : machineStringStats.keySet()) {
			bw.write(key + " " + machineStringStats.get(key) + " false\n");
		}
		bw.close();
	}
	
	@SuppressWarnings("rawtypes")
	public static void writeVertexValues(FileSystem fileSystem, Graph graphPartition,
		String outputFile) throws IOException {
		BufferedWriter bw = getBufferedWriter(fileSystem, outputFile);
		int originalIdOfLocalId;
		for (int localId = 0; localId < graphPartition.size(); ++localId) {
			originalIdOfLocalId = graphPartition.getOriginalIdOfLocalId(localId);
			if (originalIdOfLocalId >= 0) {
				bw.write("" + originalIdOfLocalId + " "
					+ graphPartition.getValueOfLocalId(localId) + "\n");
			}
		}
		bw.close();
	}

    // Note(hongsup): Adding helper functions to parse byte array.
    // TODO(semih, hongsup): Test that these helper methods work correctly. Semih just
	// fixed a few compiler bugs but didn't test that they work.
    public static int byteArrayToIntBigEndian(byte[] array, int idx) {
        int val = 0;
        val = ((array[idx+0] & 0xff) << 24) |
              ((array[idx+1] & 0xff) << 16) |
              ((array[idx+2] & 0xff) << 8) |
              ((array[idx+3] & 0xff) << 0) ;
        return val;
    }

    public static short byteArrayToShortBigEndian(byte[] array, int idx) {
        short val = 0;
        val =  (short) (((array[idx+0] & 0xff) << 8) | ((array[idx+1] & 0xff) << 0));
        return val;
    }

    public static float byteArrayToFloatBigEndian(byte[] array, int idx) {
        float val = 0;
        val = Float.intBitsToFloat(
              ((array[idx+0] & 0xff) << 24) |
              ((array[idx+1] & 0xff) << 16) |
              ((array[idx+2] & 0xff) << 8) |
              ((array[idx+3] & 0xff) << 0) );

        return val;
    }

    public static boolean byteArrayToBooleanBigEndian(byte[] array, int idx) {
        boolean b = (array[idx] == 0) ? false : true;
        return b;
    }

    public static long byteArrayToLongBigEndian(byte[] array, int idx) {
        long val = 0;
        val = ((array[idx+0] & 0xff) << 56) |
              ((array[idx+1] & 0xff) << 48) |
              ((array[idx+2] & 0xff) << 40) |
              ((array[idx+3] & 0xff) << 32) |
              ((array[idx+4] & 0xff) << 24) |
              ((array[idx+5] & 0xff) << 16) |
              ((array[idx+6] & 0xff) << 8) |
              ((array[idx+7] & 0xff) << 0) ;
        return val;
    }

    public static  double byteArrayToDoubleBigEndian(byte[] array, int idx) {
        double val = 0;
        val = Double.longBitsToDouble( 
              ((long) (array[idx+0] & 0xff) << 56) |
              ((long) (array[idx+1] & 0xff) << 48) |
              ((long) (array[idx+2] & 0xff) << 40) |
              ((long) (array[idx+3] & 0xff) << 32) |
              ((long) (array[idx+4] & 0xff) << 24) |
              ((long) (array[idx+5] & 0xff) << 16) |
              ((long) (array[idx+6] & 0xff) << 8) |
              ((long) (array[idx+7] & 0xff) << 0));
        return val;
    }

    // hongsup: Adding helper function to parse 'other command line options'
    // This function parses the given command line and fills in the hash-map with <key, value> pair.
    // All the prefix "-" are omitted in the hash-map key.
	public static void parseOtherOptions(CommandLine line, java.util.HashMap<String, String> option_map) {
		String otherOptsStr = line.getOptionValue(GPSNodeRunner.OTHER_OPTS_OPT_NAME);
		if (otherOptsStr != null) {
			String[] split = otherOptsStr.split("###");
			for (int index = 0; index < split.length; ) {
				String flag = split[index++];
				String value = split[index++];
                while (flag.startsWith("-")) {
                    flag = flag.substring(1);
                }
                option_map.put(flag, value);
            }
        }
    }

	public static List<String> getHadoopConfFiles(CommandLine line) {
		if (!line.hasOption(Utils.HADOOP_CONF_FILES_OPT_NAME)) {
			logger.error("You must specify the hadoop conf flag by setting either the " 
				+ Utils.HADOOP_CONF_FILES_OPT_NAME + " or its short version "
				+ Utils.HADOOP_CONF_FILES_SHORT_OPT_NAME + " command line argument.");
			System.exit(-1);
		}
		String optionValue = line.getOptionValue(Utils.HADOOP_CONF_FILES_OPT_NAME);
		String[] split = optionValue.split(" ");
		ArrayList<String> retVal = new ArrayList<String>();
		for (String hadoopConfFile : split) {
			retVal.add(hadoopConfFile);
		}
		return retVal;
	}

	public static String getStatsLoggingHeader(String loggingHeaderStr) {
		return Utils.LOGGER_HEADER_FIRST_PART + loggingHeaderStr + Utils.LOGGER_HEADER_SECOND_PART;
	}

	public static BufferedWriter getBufferedWriter(FileSystem fileSystem, String outputFile)
		throws IOException {
		return new BufferedWriter(
			new OutputStreamWriter(fileSystem.create(new Path(outputFile))));
	}
}
