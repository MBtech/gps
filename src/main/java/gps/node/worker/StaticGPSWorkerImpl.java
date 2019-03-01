package gps.node.worker;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import gps.communication.MessageSenderAndReceiverFactory;
import gps.graph.Graph;
import gps.graph.VertexFactory;
import gps.messages.storage.ArrayBackedIncomingMessageStorage;
import gps.node.GPSJobConfiguration;
import gps.node.MachineConfig;
import gps.writable.MinaWritable;

/**
 * The static implementation of {@link AbstractGPSWorker}.
 * 
 * @author semihsalihoglu
 */
public class StaticGPSWorkerImpl<V extends MinaWritable, E extends MinaWritable,
	M extends MinaWritable> extends AbstractGPSWorker<V, E, M> {

	private static Logger logger = Logger.getLogger(StaticGPSWorkerImpl.class);

	private StaticGPSMessageSender messageSender;

	private DataAndControlMessagesParserThread<M> dataAndControlMessagesParserThread;
	
	public StaticGPSWorkerImpl(int localMachineId, CommandLine commandLine, FileSystem fileSystem,
		MachineConfig machineConfig, Graph<V, E> graphPartition,
		VertexFactory<V, E, M> vertexFactory,
		int graphSize, int outgoingDataBufferSizes, String outputFileName,
		MessageSenderAndReceiverFactory messageSenderAndReceiverFactory,
		ArrayBackedIncomingMessageStorage<M> incomingMessageStorage, long pollingTime,
		int maxMessagesToTransmitConcurrently, int numVerticesFrequencyToCheckOutgoingBuffers,
		int sleepTimeWhenOutgoingBuffersExceedThreshold, Class<M> representativeMessageClass,
		int largeVertexPartitioningOutdegreeThreshold, boolean runPartitioningSuperstep,
		boolean combine, Class<M> messageRepresentativeInstance, Class<E> representativeEdgeClass,
		GPSJobConfiguration jobConfiguration, int numProcessorsForHandlingIO,
		boolean isNoDataParsing) throws InstantiationException, IllegalAccessException {
		super(localMachineId, commandLine, fileSystem, machineConfig, graphPartition, vertexFactory,
			graphSize, outgoingDataBufferSizes, outputFileName, messageSenderAndReceiverFactory,
			incomingMessageStorage, pollingTime, maxMessagesToTransmitConcurrently,
			numVerticesFrequencyToCheckOutgoingBuffers,
			sleepTimeWhenOutgoingBuffersExceedThreshold, largeVertexPartitioningOutdegreeThreshold,
			runPartitioningSuperstep, combine, messageRepresentativeInstance,
			representativeEdgeClass, jobConfiguration, numProcessorsForHandlingIO);
		System.out.println("Inside Static GPSWorkerImpl: " + runPartitioningSuperstep);
		this.messageSender = new StaticGPSMessageSender(machineConfig, outgoingDataBufferSizes,
			messageSenderAndReceiver);
		this.dataAndControlMessagesParserThread = new DataAndControlMessagesParserThread<M>(incomingMessageStorage,
			incomingBufferedDataAndControlMessages, controlMessageStats, representativeMessageClass,
			graphPartition, vertex, representativeEdgeClass.newInstance(), jobConfiguration,
			isNoDataParsing);
	}

	protected void startMessageParserThreads() {
		this.dataAndControlMessagesParserThread.start();
	}

	@Override
	protected StaticGPSMessageSender getMessageSender() {
		return messageSender;
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}

	@Override
	protected void finishedParsingInputSplits() {
		// TODO(semih): This is a bad hack to test initial vertex partitioning.
		this.dataAndControlMessagesParserThread.setIncomingMessageStorage(this.incomingMessageStorage);
	}
}