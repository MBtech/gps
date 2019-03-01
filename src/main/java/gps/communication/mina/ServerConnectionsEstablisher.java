package gps.communication.mina;

import gps.node.ControlMessagesStats;
import gps.node.MachineConfig;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.log4j.Logger;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

public abstract class ServerConnectionsEstablisher extends BaseConnectionsEstablisher {

	private static Logger logger = Logger.getLogger(ServerConnectionsEstablisher.class);
	private NioSocketAcceptor acceptor;
	private InetSocketAddress socketAddress;
	private int port;

	public ServerConnectionsEstablisher(MachineConfig machineConfig, int localMachineId,
		BaseMinaMessageSenderAndReceiver baseMinaMessageSenderAndReceiver,
		ControlMessagesStats controlMessageStats,
		GPSNodeExceptionNotifier gpsNodeExceptionNotifier, int numProcessorsForHandlingIO) {
		super(machineConfig, localMachineId, baseMinaMessageSenderAndReceiver, controlMessageStats,
			gpsNodeExceptionNotifier, numProcessorsForHandlingIO);
		this.acceptor = new NioSocketAcceptor(numProcessorsForHandlingIO);
		this.port = (int) machineConfig.getHostPortPair(localMachineId).snd;
		this.socketAddress = new InetSocketAddress(this.port);
	}

	@Override
	public void run() {
		configureSessionConfig(acceptor.getSessionConfig());
		configureFilterChain(acceptor.getFilterChain());
		this.acceptor.setHandler(getIoHandler());
		try {
			logger.info("Trying to bind to port: " + port);
			acceptor.bind(socketAddress);
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("Error when binding to port: "
				+ machineConfig.getHostPortPair(localMachineId).snd + ". Exiting the system.");
			System.exit(-1);
		}
	}
	
	public void cleanUp() {
		logger.info("Unbinding from hostName: " + this.socketAddress.getHostName()
			+ " port:" + this.socketAddress.getPort());
		acceptor.unbind(this.socketAddress);
	}

	protected abstract IoHandler getIoHandler();
}