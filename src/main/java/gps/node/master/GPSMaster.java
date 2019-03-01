package gps.node.master;

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import gps.communication.BaseMessageSenderAndReceiver;
import gps.communication.MessageSenderAndReceiverFactory;
import gps.globalobjects.GlobalObject;
import gps.globalobjects.GlobalObjectsMap;
import gps.globalobjects.IntSumGlobalObject;
import gps.graph.Master;
import gps.messages.MessageTypes;
import gps.messages.OutgoingBufferedMessage;
import gps.node.AbstractGPSNode;
import gps.node.ControlMessagesStats;
import gps.node.GPSNodeRunner;
import gps.node.InputSplit;
import gps.node.MachineConfig;
import gps.node.MachineStats;
import gps.node.StatusType;
import gps.node.Utils;
import gps.node.ControlMessagesStats.ControlMessageType;
import gps.node.MachineStats.StatName;
import gps.node.master.monitoring.Server;
import gps.node.master.monitoring.ServerHandler;
import gps.node.worker.GPSWorkerExposedGlobalVariables;
import gps.writable.MinaWritable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.mina.core.buffer.IoBuffer;

public class GPSMaster extends AbstractGPSNode {

	public static final int DEFAULT_MONITORING_PORT = 8888;

	private static Logger logger = Logger.getLogger(GPSMaster.class);

	private BaseMessageSenderAndReceiver messageSenderAndReceiverForMaster;
	private final ControlMessagesStats controlMessageStats;
	private final MachineStats machineStatsForMaster;
	private final MachineConfig machineConfig;
	private static int superstepNo = -1;
	private final boolean isDynamic;
	private final String machineStatsOutputFileName;
	private final Master master;
	private final String masterOutputFileName;

	public GPSMaster(FileSystem fileSystem, MachineConfig machineConfig,
		MessageSenderAndReceiverFactory messageSenderAndReceiverFactory, long pollingTime,
		String machineStatsOutputFileName, CommandLine line, boolean isDynamic,
		Master master, String masterOutputFileName, int numProcessorsForHandlingIO) {
		super(fileSystem, pollingTime);
		this.machineConfig = machineConfig;
		this.machineStatsOutputFileName = machineStatsOutputFileName;
		this.isDynamic = isDynamic;
		this.masterOutputFileName = masterOutputFileName;
		logger.info("isDynamic: " + isDynamic);
		this.controlMessageStats = new ControlMessagesStats();
		this.machineStatsForMaster = new MachineStats();
		putCommandLineOptionsToMachineStats(line);
		putJVMOptionsIntoMachineStats();
		this.messageSenderAndReceiverForMaster =
			messageSenderAndReceiverFactory.newInstanceForMaster(machineConfig, Utils.MASTER_ID,
				controlMessageStats, machineStatsForMaster, pollingTime, gpsWorkerMessages,
				gpsNodeExceptionNotifier, numProcessorsForHandlingIO);
		this.master = master;
		this.master.machineStatsForMaster = this.machineStatsForMaster;
		GPSWorkerExposedGlobalVariables.initVariables(-1, machineConfig, -1, -1);
	}

	private void putJVMOptionsIntoMachineStats() {
		List<String> jvmArgumentList = ManagementFactory.getRuntimeMXBean().getInputArguments();
		for (int i = 0; i < jvmArgumentList.size(); i++) {
			machineStatsForMaster.getMachineStringStats().put(Utils.JVM_ARGS_PREFIX + i,
				jvmArgumentList.get(i));
		}
	}

	private void putCommandLineOptionsToMachineStats(CommandLine line) {
		int commandLineArgsCounter = 0;
		for (Option option : line.getOptions()) {
			machineStatsForMaster.getMachineStringStats().put(Utils.COMMAND_LINE_STAT_PREFIX +
				commandLineArgsCounter++,
				option.getLongOpt() + Utils.COMMAND_LINE_STAT_VALUE_SEPARATOR + option.getValue());
		}
		commandLineArgsCounter = putDefaultOptValueToMachineStatsIfNotExists(line,
			commandLineArgsCounter, GPSNodeRunner.OUTGOING_DATA_BUFFER_SIZES_OPT_NAME,
			GPSNodeRunner.DEFAULT_OUTGOING_BUFFER_SIZES);
		commandLineArgsCounter = putDefaultOptValueToMachineStatsIfNotExists(line,
			commandLineArgsCounter, GPSNodeRunner.MAX_MESSAGES_TO_TRANSMIT_CONCURRENTLY_OPT_NAME,
			GPSNodeRunner.DEFAULT_MAX_MESSAGES_TO_TRANSMIT_CONCURRENTLY);
		commandLineArgsCounter = putDefaultOptValueToMachineStatsIfNotExists(line,
			commandLineArgsCounter, GPSNodeRunner.NUM_VERTICES_FREQUENCY_TO_CHECK_OUTGOING_BUFFERS_OPT_NAME,
			GPSNodeRunner.DEFAULT_NUM_VERTICES_FREQUENCY_TO_CHECK_OUTGOING_BUFFERS);
		commandLineArgsCounter = putDefaultOptValueToMachineStatsIfNotExists(line,
			commandLineArgsCounter, GPSNodeRunner.SLEEP_TIME_WHEN_OUTGOING_BUFFERS_EXCEED_THRESHOLD_OPT_NAME,
			GPSNodeRunner.DEFAULT_SLEEP_TIME_WHEN_OUTGOING_BUFFERS_EXCEED_THRESHOLD);		
		commandLineArgsCounter = putDefaultOptValueToMachineStatsIfNotExists(line,
			commandLineArgsCounter, GPSNodeRunner.DYNAMISM_BENEFIT_THRESHOLD_OPT_NAME,
			GPSNodeRunner.DEFAULT_DYNAMISM_BENEFIT_THRESHOLD);
		commandLineArgsCounter = putDefaultOptValueToMachineStatsIfNotExists(line,
			commandLineArgsCounter, GPSNodeRunner.DYNAMISM_EDGE_THRESHOLD_OPT_NAME,
			GPSNodeRunner.DEFAULT_EDGE_THRESHOLD);
		commandLineArgsCounter = putDefaultOptValueToMachineStatsIfNotExists(line,
			commandLineArgsCounter, GPSNodeRunner.SUPERSTEP_NO_TO_STOP_DYNAMISM_OPT_NAME,
			GPSNodeRunner.DEFAULT_SUPERSTEP_NO_TO_STOP_DYNAMISM);
	}

	private int putDefaultOptValueToMachineStatsIfNotExists(CommandLine line,
		int commandLineArgsCounter, String optName, int defaultValue) {
		if (!line.hasOption(optName)) {
			machineStatsForMaster.getMachineStringStats().put(Utils.COMMAND_LINE_STAT_PREFIX +
				commandLineArgsCounter++,
				optName + Utils.COMMAND_LINE_STAT_VALUE_SEPARATOR + defaultValue);
		}
		return commandLineArgsCounter;
	}

	public void startMaster(CommandLine line) throws Throwable {
		machineStatsForMaster.updateGlobalStat(StatName.SYSTEM_START_TIME,
			System.currentTimeMillis());
		machineStatsForMaster.updateLatestStatus(Utils.MASTER_ID, StatusType.STARTING_UP);
		logger.info("Starting GPS Master...");
		logger.info("Starting GPS Master Monitoring Http Server at port: "
			+ DEFAULT_MONITORING_PORT);
		new Server(new ServerHandler(machineConfig, machineStatsForMaster),
			DEFAULT_MONITORING_PORT).start();
		messageSenderAndReceiverForMaster.startEstablishingAllConnections();
		machineStatsForMaster.updateLatestStatus(Utils.MASTER_ID,
			StatusType.ESTABLISHING_TCP_CONNECTIONS);
		messageSenderAndReceiverForMaster.finishEstablishingAllConnections();

		partitionInputFiles(line.getOptionValue(GPSNodeRunner.INPUT_FILES_OPT_NAME));
		waitForAllControlMessagesToBeReceivedOrSent(0, controlMessageStats,
			ControlMessageType.RECEIVED_GLOBAL_OBJECTS_MESSAGES,
			machineConfig.getWorkerIds().size(), machineStatsForMaster);
		GlobalObjectsMap globalObjectsMap = parseGlobalObjectsMessages(superstepNo,
			null /* don't add anything to machineStats */);
		waitForAllControlMessagesToBeReceivedOrSent(-1,
			controlMessageStats, ControlMessageType.READY_TO_START_COMPUTATION,
			machineConfig.getWorkerIds().size(), machineStatsForMaster);
		machineStatsForMaster.updateGlobalStat(StatName.START_TIME, System.currentTimeMillis());
		superstepNo = 1;
		long superstepStartTime = System.currentTimeMillis();
		do {
			logger.info("Starting superstepNo: " + superstepNo);
			superstepStartTime = System.currentTimeMillis();
			Master.globalObjectsMap = globalObjectsMap;
			setMasterGraphSize(globalObjectsMap);
			dumpGlobalObjects(globalObjectsMap, superstepNo);
			master.compute(superstepNo);
			// TODO(semih): We do a redundant thing here by sending global objects
			// to workers even if we're going to stop computation.
			renameDefaultGlobalObjects(globalObjectsMap);
			sendGlobalObjectsMessageToAllWorkers(globalObjectsMap);
			logger.info("Sending startNextSuperstepOrOutputAndTerminateControlMessages."
				+ " continueComputation: " + master.continueComputation);
			if (isDynamic) {
				sendStartSuperstepOrTerminateMessagesForDynamic(master.continueComputation);
			} else {
				sendStartSuperstepOrTerminateMessagesForStatic(master.continueComputation);
			}
			machineStatsForMaster.updateLatestStatus(Utils.MASTER_ID,
				StatusType.WAITING_FOR_BEGIN_NEXT_SUPERSTEP_OR_TERMINATE_MESSAGES_TO_BE_SENT_TO_WORKERS);
			waitForAllControlMessagesToBeReceivedOrSent(superstepNo, controlMessageStats,
				ControlMessageType.SENT_BEGIN_NEXT_SUPERSTEP_OR_TERMINATE, machineConfig
					.getWorkerIds().size(), machineStatsForMaster);
			// TODO(semih): Remove the below two lines. They're just for debugging purposes.
//			machineStatsForMaster.logGlobalStats();
//			machineStatsForMaster.logStatsForSuperstepForAllWorkers(superstepNo, machineConfig);
			if (!master.continueComputation) {
				break;
			}
//			machineStatsForMaster.logStatsForSuperstepForAllWorkers(superstepNo, machineConfig);
			machineStatsForMaster.updateLatestStatus(Utils.MASTER_ID,
				StatusType.WAITING_FOR_END_OF_SUPERSTEP_MESSAGES_FROM_WORKERS);
			waitForAllControlMessagesToBeReceivedOrSent(superstepNo, controlMessageStats,
				ControlMessageType.RECEIVED_GLOBAL_OBJECTS_MESSAGES,
				machineConfig.getWorkerIds().size(), machineStatsForMaster);
			globalObjectsMap = parseGlobalObjectsMessages(superstepNo, machineStatsForMaster);
			waitForAllControlMessagesToBeReceivedOrSent(superstepNo, controlMessageStats,
				ControlMessageType.RECEIVED_END_OF_SUPERSTEP_MESSAGES,
				machineConfig.getWorkerIds().size(), machineStatsForMaster);
			machineStatsForMaster.updateStat(StatName.TOTAL_TIME, superstepNo,
				(double) (System.currentTimeMillis() - superstepStartTime), Utils.MASTER_ID);
			logger.info("total time for superstepNo: " + superstepNo + ": " +
				(System.currentTimeMillis() - superstepStartTime));
			superstepNo++;
		} while (true);

		BufferedWriter masterBufferedWriter = Utils.getBufferedWriter(fileSystem, masterOutputFileName);
		master.writeOutput(masterBufferedWriter);
		masterBufferedWriter.close();
		Utils.writeMachineStats(fileSystem, machineStatsForMaster, machineStatsOutputFileName);
		logger.info("Finished Computation. Total Time: "+ (System.currentTimeMillis()
			- machineStatsForMaster.getStatValue(StatName.START_TIME) + "\nShutting down..."));
		messageSenderAndReceiverForMaster.closeServerSocket();
		System.exit(-1);
	}

	private void setMasterGraphSize(GlobalObjectsMap globalObjectsMap) {
		GlobalObject<? extends MinaWritable> numVerticesGlobalObject =
			globalObjectsMap.getGlobalObject(GlobalObjectsMap.NUM_VERTICES);
		logger.info("Setting num vertices...");
		if (numVerticesGlobalObject != null) {
			master.setGraphSize(((IntSumGlobalObject) numVerticesGlobalObject).getValue().getValue());			
			logger.info("num vertices is: " + master.getGraphSize());
		} else {
			logger.info("num vertices is null");
		}	
	}

	private void partitionInputFiles(String inputFilesStr) throws IOException {
		String[] files = inputFilesStr.split("\\s+");
		logger.info("Num files to partition: " + files.length);
		long totalBytes = getTotalBytes(files);
		long bytesPerEachWorker = (totalBytes / machineConfig.getWorkerIds().size()) + 1;
		logger.info("totalBytes to partition: " + totalBytes + " bytesPerWorker: "
			+ bytesPerEachWorker);
		List<List<InputSplit>> inputSplitsPerWorker = new ArrayList<List<InputSplit>>();
		for (int i = 0; i < machineConfig.getWorkerIds().size(); ++i) {
			inputSplitsPerWorker.add(new ArrayList<InputSplit>());
		}

		int nextWorkerId = 0;
		long numBytesLeftForNextWorkerId = bytesPerEachWorker;
		for (String file : files) {
			FileStatus[] fileStatusArray = fileSystem.globStatus(new Path(file));
			for (FileStatus fileStatus : fileStatusArray) {
				long totalFileLength = fileStatus.getLen();
				long nextFileOffset = 0;
				logger.info("Starting to parse file: " + fileStatus.getPath().toUri().getPath()
					+ " totalLength: " + totalFileLength);
				while (nextFileOffset < totalFileLength) {
					logger.info("Adding a split to worker: " + nextWorkerId
						+ " numBytesLeftForNextWorkerId: " + numBytesLeftForNextWorkerId
						+ " nextFileOffset: " + nextFileOffset);
					long startOffset = nextFileOffset;
					long bytesForThisSplit =
						Math.min(totalFileLength - startOffset, numBytesLeftForNextWorkerId);
					inputSplitsPerWorker.get(nextWorkerId).add(
						new InputSplit(fileStatus.getPath().toUri().getPath(), startOffset, startOffset + bytesForThisSplit - 1));
					nextFileOffset += bytesForThisSplit;
					numBytesLeftForNextWorkerId -= bytesForThisSplit;
					if (numBytesLeftForNextWorkerId == 0) {
						logger.info("no bytes left for this worker. moving to next worker id: "
							+ (nextWorkerId + 1));
						nextWorkerId++;
						numBytesLeftForNextWorkerId = bytesPerEachWorker;
					}
				}
			}
		}
		sendInputSplitMessagesToAllWorker(inputSplitsPerWorker);
		dumpInputSplitsPerMachine(inputSplitsPerWorker);
	}

	private void dumpInputSplitsPerMachine(List<List<InputSplit>> inputSplitsPerWorker) {
		logger.info("Start of dumping input splits per machine...");
		for (int i = 0; i < machineConfig.getWorkerIds().size(); ++i) {
			logger.info("Input splits for workerId: " + i);
			List<InputSplit> inputSplits = inputSplitsPerWorker.get(i);
			for (InputSplit inputSplit : inputSplits) {
				logger.info("fileName: " + inputSplit.getFileName() + " startOffset: "
					+ inputSplit.getStartOffset() + " endOffset: " + inputSplit.getEndOffset());
			}
			logger.info("End of input splits for workerId: " + i);
		}
		logger.info("End of dumping input splits per machine...");
	}

	private long getTotalBytes(String[] files) throws IOException {
		long totalBytes = 0;
		for (String file : files) {
			FileStatus[] fileStatusArray = fileSystem.globStatus(new Path(file));
			logger.info("numFiles matching path: " + file  + ": " + fileStatusArray.length);
			for (FileStatus fileStatus : fileStatusArray) {
				totalBytes += fileStatus.getLen();
			}
		}
		return totalBytes;
	}

	private void renameDefaultGlobalObjects(GlobalObjectsMap globalObjectsMap) {
		renameGlobalObject(globalObjectsMap, GlobalObjectsMap.NUM_VERTICES,
			GlobalObjectsMap.NUM_TOTAL_VERTICES);
		renameGlobalObject(globalObjectsMap, GlobalObjectsMap.NUM_EDGES,
			GlobalObjectsMap.NUM_TOTAL_EDGES);
		renameGlobalObject(globalObjectsMap, GlobalObjectsMap.NUM_ACTIVE_VERTICES,
			GlobalObjectsMap.NUM_TOTAL_ACTIVE_VERTICES);
	}

	private void renameGlobalObject(GlobalObjectsMap globalObjectsMap, String oldKey,
		String newKey) {
		GlobalObject<? extends MinaWritable> bv = globalObjectsMap.getGlobalObject(oldKey);
		if (bv == null) {
			logger.info("Returning from renameGlobalObjects because bv is null.");
			return;
		}
		logger.info("renaming " + oldKey + " to " + newKey + " value: " + bv.getValue());
		globalObjectsMap.removeGlobalObject(oldKey);
		globalObjectsMap.putGlobalObject(newKey, bv);
	}

	private void sendInputSplitMessagesToAllWorker(List<List<InputSplit>> inputSplitsPerWorker)
		throws CharacterCodingException {
		int[] randomPermutation = Utils.getRandomPermutation(machineConfig.getWorkerIds().size());
		List<Integer> allMachineIds = new LinkedList<Integer>(machineConfig.getWorkerIds());
		for (int i : randomPermutation) {
			int toMachineId = allMachineIds.get(i);
			List<InputSplit> inputSplitList = inputSplitsPerWorker.get(i);
			for (InputSplit inputSplit : inputSplitList) {
				logger.info("Sending inputSplit toMachineId:"
					+ toMachineId + " fileName: " + inputSplit.getFileName() + " startOffset: "
					+ inputSplit.getStartOffset() + " endOffset: " + inputSplit.getEndOffset());
			}
			messageSenderAndReceiverForMaster.sendBufferedMessage(
				constructInputSplitMessage(inputSplitList), toMachineId);
		}
	}

	private OutgoingBufferedMessage constructInputSplitMessage(List<InputSplit> inputSplits)
		throws CharacterCodingException {
		IoBuffer ioBuffer = IoBuffer.allocate(10).setAutoExpand(true);
		for (InputSplit inputSplit : inputSplits) {
			ioBuffer.putString(inputSplit.getFileName(), Utils.ISO_8859_1_ENCODER);
			ioBuffer.putChar('\u0000');
			ioBuffer.putLong(inputSplit.getStartOffset());
			ioBuffer.putLong(inputSplit.getEndOffset());
		}
		return new OutgoingBufferedMessage(MessageTypes.INPUT_SPLIT, -1, ioBuffer);
	}

	private void sendGlobalObjectsMessageToAllWorkers(GlobalObjectsMap globalObjectsMap)
		throws CharacterCodingException {
		int[] randomPermutation = Utils.getRandomPermutation(machineConfig.getWorkerIds().size());
		List<Integer> allMachineIds = new LinkedList<Integer>(machineConfig.getWorkerIds());
		for (int i : randomPermutation) {
			int toMachineId = allMachineIds.get(i);
			logger.debug("Sending " + MessageTypes.GLOBAL_OBJECTS + " message toMachineId:"
				+ toMachineId + " superstepNo: " + superstepNo);
			messageSenderAndReceiverForMaster.sendBufferedMessage(
				constructGlobalObjectsMessage(superstepNo, globalObjectsMap),
				toMachineId);
		}
	}

	private void sendStartSuperstepOrTerminateMessagesForStatic(boolean continueComputation) {
		List<Integer> workerIds = machineConfig.getWorkerIds();
		int numWorkers = workerIds.size();
		int[] randomPermutation = Utils.getRandomPermutation(numWorkers);
		for (int i : randomPermutation) {
			int toMachineId = workerIds.get(i);
			logger.debug("Sending static " + MessageTypes.BEGIN_NEXT_SUPERSTEP_OR_TERMINATE +
				" message toMachineId:" + toMachineId);
			IoBuffer ioBuffer = IoBuffer.allocate(1);
			ioBuffer.put(continueComputation ? (byte) 1 : (byte) 0);
			messageSenderAndReceiverForMaster.sendBufferedMessage(new OutgoingBufferedMessage(
				MessageTypes.BEGIN_NEXT_SUPERSTEP_OR_TERMINATE, superstepNo, ioBuffer),
				toMachineId);
		}
	}

	private void sendStartSuperstepOrTerminateMessagesForDynamic(
		boolean continueComputation) {
		List<Integer> workerIds = machineConfig.getWorkerIds();
		int numWorkers = workerIds.size();
		int[] randomPermutation = Utils.getRandomPermutation(numWorkers);
		IoBuffer ioBuffer;
		for (int i : randomPermutation) {
			int toMachineId = workerIds.get(i);
			logger.debug("Sending dynamic " + MessageTypes.BEGIN_NEXT_SUPERSTEP_OR_TERMINATE +
				" message toMachineId:" + toMachineId);
			ioBuffer = IoBuffer.allocate(1);
			ioBuffer.put(continueComputation ? (byte) 1 : (byte) 0);
			messageSenderAndReceiverForMaster.sendBufferedMessage(new OutgoingBufferedMessage(
				MessageTypes.BEGIN_NEXT_SUPERSTEP_OR_TERMINATE, superstepNo, ioBuffer),
				toMachineId);
		}
	}

	public static OutgoingBufferedMessage constructStartNextSuperstepOrOutputAndTerminateMessage(
		int superstepNo, boolean startNextSuperstep) {
		IoBuffer ioBuffer = IoBuffer.allocate(1);
		ioBuffer.put(startNextSuperstep ? (byte) 1 : (byte) 0);
		return new OutgoingBufferedMessage(
			MessageTypes.BEGIN_NEXT_SUPERSTEP_OR_TERMINATE, superstepNo, ioBuffer);
	}

	public static int currentSuperstepNo() {
		return superstepNo;
	}
}
