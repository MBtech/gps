package gps.node;

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.mina.core.buffer.IoBuffer;

import gps.communication.mina.GPSNodeExceptionNotifier;
import gps.globalobjects.GlobalObject;
import gps.globalobjects.GlobalObjectsMap;
import gps.globalobjects.IntOverwriteGlobalObject;
import gps.messages.IncomingBufferedMessage;
import gps.messages.MessageTypes;
import gps.messages.OutgoingBufferedMessage;
import gps.node.ControlMessagesStats.ControlMessageType;
import gps.writable.DoubleWritable;
import gps.writable.FloatWritable;
import gps.writable.IntWritable;
import gps.writable.LongWritable;
import gps.writable.MinaWritable;

public abstract class AbstractGPSNode {

	private static Logger logger = Logger.getLogger(AbstractGPSNode.class);
	protected volatile GPSNodeExceptionNotifier gpsNodeExceptionNotifier;
	protected final BlockingQueue<IncomingBufferedMessage> gpsWorkerMessages;
	protected final long controlMessagesPollingTime;
	protected FileSystem fileSystem;

	public AbstractGPSNode(FileSystem fileSystem, long controlMessagesPollingTime) {
		this.controlMessagesPollingTime = controlMessagesPollingTime;
		this.gpsNodeExceptionNotifier = new GPSNodeExceptionNotifier();
		this.gpsWorkerMessages = new LinkedBlockingQueue<IncomingBufferedMessage>();
		this.fileSystem = fileSystem;
	}

	protected void waitForAllControlMessagesToBeReceivedOrSent(int superstepNo,
		ControlMessagesStats controlMessageStats, ControlMessageType controlMessageType,
		int expectedNumControlMessages, MachineStats machineStats) throws Throwable {
		long timeBeforeWaitingForAllControlMessages = System.currentTimeMillis();
		boolean done = false;
		while (!done) {
			// TODO(semih): Change this!! It is dangerous that isPerSuperstep flag is
			// read from the StatName as opposed to ControlMessageType, which can cause
			// and has caused copy-paste errors where the StatName was not accurate and caused
			// a bug to be introduced.
			if (controlMessageType.getStatName().isPerSuperstep()) {
				controlMessageStats.logPerSuperstepControlMessages(superstepNo, controlMessageType);
			} else {
				controlMessageStats.logGlobalControlMessages(controlMessageType);
			}
			if (controlMessageType.getStatName().isPerSuperstep()) {
				done = controlMessageStats.hasReceivedAllPerSuperstepControlMessages(
					superstepNo, controlMessageType, expectedNumControlMessages);
			} else {
				done = controlMessageStats.hasReceivedAllGlobalControlMessages(controlMessageType,
					expectedNumControlMessages);
			}
			if (!done) {
				logger.debug("Have not " + controlMessageType + ". superstepNo: "
					+ superstepNo + ". Sleeping for " + controlMessagesPollingTime + " millis...");
				throwExceptionIfMINAThrewException();
				Thread.sleep(controlMessagesPollingTime);
			}
		}
		long waitTimeForControlMessage =
			System.currentTimeMillis() - timeBeforeWaitingForAllControlMessages;
		if (controlMessageType.getStatName().isPerSuperstep()) {
			machineStats.updateStatForSuperstep(controlMessageType.getStatName(),
				superstepNo, (double) waitTimeForControlMessage);
		} else {
			machineStats.updateGlobalStat(controlMessageType.getStatName(),
				waitTimeForControlMessage);
		}
	}

	protected GlobalObjectsMap parseGlobalObjectsMessages(int superstepNo,
		MachineStats machineStatsForAddingGlobalObjects) throws InterruptedException,
		CharacterCodingException, ClassNotFoundException, InstantiationException,
		IllegalAccessException {
		GlobalObjectsMap globalObjectsMap = new GlobalObjectsMap();
		logger.debug("Parsing GlobalObjects... gpsWorkerMessagesSize: " + gpsWorkerMessages.size());
		while (!gpsWorkerMessages.isEmpty()) {
			IncomingBufferedMessage incomingBufferedMessage = gpsWorkerMessages.take();
			IoBuffer ioBuffer = incomingBufferedMessage.getIoBuffer();
			while (ioBuffer.hasRemaining()) {
				// IoBuffer.getString reads multiple putStrings as a single string.
				String string = ioBuffer.getString(Utils.ISO_8859_1_DECODER);
				logger.debug("string from ioBuffer: " + string);
				String[] split = string.split(
					Utils.COMMAND_LINE_STAT_VALUE_SEPARATOR);
				String key = split[0];
				String className = split[1];
				logger.debug("key: " + key + " className: " + className);
				GlobalObject<MinaWritable> reflectedBV =
		        (GlobalObject<MinaWritable>)
		            ((Class<? extends GlobalObject<MinaWritable>>)
		            	((Class<? extends GlobalObject<MinaWritable>>)
					    Class.forName(className))).newInstance();
				logger.debug("ioBuffer.position(): " + ioBuffer.position());
				logger.debug("ioBuffer.limit(): " + ioBuffer.limit());
				// TODO(semih): Understand why there is a move by 1 in ioBuffer when reading
				// string. Fix this +1. This is an ugly hack
				ioBuffer.position(ioBuffer.position() + 1);
				reflectedBV.getValue().read(ioBuffer);
				if (machineStatsForAddingGlobalObjects != null) {
					addGlobalObjectToMachineStats(reflectedBV, key, superstepNo,
						incomingBufferedMessage.getFromMachineId(), machineStatsForAddingGlobalObjects);
				}
				globalObjectsMap.putOrUpdateGlobalObject(key, reflectedBV);					
			}
		}
		logger.debug("End of parsing GlobalObjects..  size: " + globalObjectsMap.keySet().size());
		return globalObjectsMap;
	}

	private void addGlobalObjectToMachineStats(GlobalObject<MinaWritable> reflectedBV, String key,
		int superstepNo, int workerId, MachineStats machineStatsForAddingGlobalObjects) {
		Double valueToExpose = null;
		if (reflectedBV.getValue() instanceof IntWritable) {
			valueToExpose = (double) ((IntWritable) reflectedBV.getValue()).getValue();
		} else if (reflectedBV.getValue() instanceof DoubleWritable) {
			valueToExpose = ((DoubleWritable) reflectedBV.getValue()).getValue();
		} else if (reflectedBV.getValue() instanceof LongWritable) {
			valueToExpose = (double) ((LongWritable) reflectedBV.getValue()).getValue();
		} else if (reflectedBV.getValue() instanceof FloatWritable) {
			valueToExpose = (double) ((FloatWritable) reflectedBV.getValue()).getValue();
		}
		if (valueToExpose != null) {
			machineStatsForAddingGlobalObjects.addCustomStatKey(key);
			machineStatsForAddingGlobalObjects.updateStat(key, superstepNo, valueToExpose,
				workerId);
		}
	}

	protected OutgoingBufferedMessage constructGlobalObjectsMessage(int superstepNo,
		GlobalObjectsMap globalObjectsMap) throws CharacterCodingException {
		IoBuffer ioBuffer = IoBuffer.allocate(1).setAutoExpand(true);
		logger.debug(Utils.LOGGER_HEADER_FIRST_PART + " Start of constructing global objects superstepNo: " + superstepNo
			+ Utils.LOGGER_HEADER_SECOND_PART);
		for (String key : globalObjectsMap.keySet()) {
			 // TODO(semih): Check that you have enough space to write these 
			logger.debug("key: " + key);
			GlobalObject<? extends MinaWritable> bv = globalObjectsMap.getGlobalObject(key);
			logger.debug("bv.value(): " + bv.getValue());
			logger.debug("IoBuffer.limit before putting key and className: " + ioBuffer.limit());
			ioBuffer.putString(key, Utils.ISO_8859_1_ENCODER);
			ioBuffer.putString(Utils.COMMAND_LINE_STAT_VALUE_SEPARATOR, Utils.ISO_8859_1_ENCODER);
			logger.debug("bv.className: " + bv.getClass().getName());
			ioBuffer.putString(bv.getClass().getName(), Utils.ISO_8859_1_ENCODER);
			ioBuffer.putChar('\u0000');
			logger.debug("IoBuffer.limit before putting bv: " + ioBuffer.limit());
			bv.getValue().write(ioBuffer);
			logger.debug("IoBuffer.limit after putting bv: " + ioBuffer.limit());
		}
		logger.debug("IoBuffer.limit(): " + ioBuffer.limit());
		logger.debug(Utils.LOGGER_HEADER_FIRST_PART + " End of constructing global objects"
			+ Utils.LOGGER_HEADER_SECOND_PART);
		return new OutgoingBufferedMessage(MessageTypes.GLOBAL_OBJECTS,
			superstepNo, ioBuffer);
	}

	protected void dumpGlobalObjects(GlobalObjectsMap globalObjectsMap,
		int superstepNo) {		
		logger.info(Utils.LOGGER_HEADER_FIRST_PART + "Start of dumping global objects " +
				"superstepNo: " + superstepNo + Utils.LOGGER_HEADER_SECOND_PART);
		List<String> keySet = new ArrayList<String>(globalObjectsMap.keySet());
		Collections.sort(keySet, new MessageBucketsComparable());
		for (String key : keySet) {
			GlobalObject<? extends MinaWritable> bv =
				globalObjectsMap.getGlobalObject(key);
			logger.info("key: " + key + " value: " + bv.getValue());
		}
		logger.info(Utils.LOGGER_HEADER_FIRST_PART + "End of dumping global objects " +
			"superstepNo: " + superstepNo + Utils.LOGGER_HEADER_SECOND_PART);
	}

	private void throwExceptionIfMINAThrewException() throws Throwable {
		if (gpsNodeExceptionNotifier.getThrowable() != null) {
			throw gpsNodeExceptionNotifier.getThrowable();
		}
	}
	
	private static class MessageBucketsComparable implements Comparator<String> {

//		@Override
		public int compare(String str1, String str2) {
			String[] split1 = str1.split("-");
			String[] split2 = str2.split("-");
			if (!split1[0].equals(split2[0])) {
				return split1[0].compareTo(split2[0]);
			} else if (str1.contains("bucket") && str2.contains("bucket")){
				int int1 = Integer.parseInt(split1[split1.length - 1]);
				int int2 = Integer.parseInt(split2[split2.length - 1]);
				if (int1 < int2) {
					return 0;
				} else {
					return 1;
				}
			}
			return str1.compareTo(str2);
		}


	}
}