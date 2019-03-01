package gps.communication.mina;

import gps.messages.MessageUtils;
import gps.node.ControlMessagesStats;
import gps.node.MachineConfig;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.future.WriteFuture;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

public abstract class ClientConnectionsEstablisher extends BaseConnectionsEstablisher {

	private static Logger logger = Logger.getLogger(ClientConnectionsEstablisher.class);
	private final long connectionEstablishingPollingTime;

	public ClientConnectionsEstablisher(MachineConfig machineConfig, int localMachineId,
		BaseMinaMessageSenderAndReceiver baseMinaMessageSenderAndReceiver,
		ControlMessagesStats controlMessageStats, long connectionFailurePollingTime,
		GPSNodeExceptionNotifier gpsNodeExceptionNotifier, int numProcessorsForHandlingIO) {
		super(machineConfig, localMachineId, baseMinaMessageSenderAndReceiver, controlMessageStats,
			gpsNodeExceptionNotifier, numProcessorsForHandlingIO);
		this.connectionEstablishingPollingTime = connectionFailurePollingTime;
	}

	@Override
	public void run() {
		NioSocketConnector connector = new NioSocketConnector(numProcessorsForHandlingIO);
		configureSessionConfig(connector.getSessionConfig());
		configureFilterChain(connector.getFilterChain());
		connector.setHandler(getIoHandler());
		int numConnections = 0;
		for (int machineId : machineConfig.getAllMachineIds()) {
			if (machineId > localMachineId) {
				numConnections++;
			}
		}
		CountDownLatch countDownLatch = new CountDownLatch(numConnections);
		List<ConnectionEstablisher> connectionEstablishers = new ArrayList<ConnectionEstablisher>();
		for (int machineId : machineConfig.getAllMachineIds()) {
			if (machineId > localMachineId) {
				String hostName = machineConfig.getHostPortPair(machineId).fst;
				int port = machineConfig.getHostPortPair(machineId).snd;
				logger.info("Trying to connect to machineId: " + machineId);
				ConnectionEstablisher connectionEstablisher =
					new ConnectionEstablisher(machineId, connector, countDownLatch, hostName, port);
				connectionEstablisher.run();
				connectionEstablishers.add(connectionEstablisher);
			}
		}
		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}

	public abstract IoHandler getIoHandler();

	private class ConnectionEstablisher implements Runnable {

		private final String hostName;
		private final int port;
		private final int machineId;
		private IoSession session;
		private final CountDownLatch countDownLatch;
		private final NioSocketConnector connector;

		private ConnectionEstablisher(int machineId, NioSocketConnector connector,
			CountDownLatch countDownLatch, String hostName, int port) {
			this.machineId = machineId;
			this.connector = connector;
			this.countDownLatch = countDownLatch;
			this.hostName = hostName;
			this.port = port;

		}

		public void run() {
			for (;;) {
				try {
					ConnectFuture future = connector.connect(new InetSocketAddress(hostName, port));
					future.awaitUninterruptibly();
					this.session = future.getSession();
					session.setAttribute("machineId", machineId);
					logger.info("Writing the local machine id message to machineId: " + machineId);
					WriteFuture writeFuture =
						session.write(MessageUtils.constructLocalMachineIdMessage(localMachineId));
					writeFuture.awaitUninterruptibly();
					baseMinaMessageSenderAndReceiver.putSession(machineId, session);
					break;
				} catch (RuntimeIoException e) {
					logger.error("Failed to connect to " + hostName + " at port: " + port
						+ ". Waiting for: " + connectionEstablishingPollingTime + " millis.");
					e.printStackTrace();
					try {
						Thread.sleep(connectionEstablishingPollingTime);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				}
			}
			countDownLatch.countDown();
		}
	}
}