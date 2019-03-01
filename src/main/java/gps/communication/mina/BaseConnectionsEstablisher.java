package gps.communication.mina;

import gps.node.ControlMessagesStats;
import gps.node.MachineConfig;

import org.apache.mina.core.filterchain.DefaultIoFilterChainBuilder;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.SocketSessionConfig;

public class BaseConnectionsEstablisher extends Thread {

	protected final MachineConfig machineConfig;
	protected final int localMachineId;
	protected final BaseMinaMessageSenderAndReceiver baseMinaMessageSenderAndReceiver;
	protected final ControlMessagesStats controlMessageStats;
	protected final GPSNodeExceptionNotifier gpsNodeExceptionNotifier;
	protected final int numProcessorsForHandlingIO;

	public BaseConnectionsEstablisher(MachineConfig machineConfig, int localMachineId,
		BaseMinaMessageSenderAndReceiver baseMinaMessageSenderAndReceiver,
		ControlMessagesStats controlMessageStats,
		GPSNodeExceptionNotifier gpsNodeExceptionNotifier, int numProcessorsForHandlingIO) {
		this.machineConfig = machineConfig;
		this.localMachineId = localMachineId;
		this.baseMinaMessageSenderAndReceiver = baseMinaMessageSenderAndReceiver;
		this.controlMessageStats = controlMessageStats;
		this.gpsNodeExceptionNotifier = gpsNodeExceptionNotifier;
		this.numProcessorsForHandlingIO = numProcessorsForHandlingIO;
	}

	protected void configureSessionConfig(SocketSessionConfig sessionConfig) {
		sessionConfig.setReadBufferSize(2048000);
		sessionConfig.setSendBufferSize(2048000);
		sessionConfig.setIdleTime(IdleStatus.BOTH_IDLE, 120);
		sessionConfig.setWriteTimeout(240);
		sessionConfig.setWriterIdleTime(240);
	}

	protected void configureFilterChain(DefaultIoFilterChainBuilder filterChain) {
		filterChain.addLast("protocol", new ProtocolCodecFilter(new MessageCodecFactory()));
	}
}
