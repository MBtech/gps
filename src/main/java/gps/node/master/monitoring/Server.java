package gps.node.master.monitoring;

import java.net.InetSocketAddress;

import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

/**
 * Entry point to HTTP monitoring server
 */
public class Server extends Thread {

	private final BaseServerHandler baseServerHandler;
	private final int port;

	public Server(BaseServerHandler baseServerHandler, int port) {
		this.baseServerHandler = baseServerHandler;
		this.port = port;
	}

	@Override
	public void run() {
		try {
			NioSocketAcceptor acceptor = new NioSocketAcceptor();
			acceptor.getFilterChain().addLast("protocolFilter",
				new ProtocolCodecFilter(new HttpServerProtocolCodecFactory()));
			acceptor.getFilterChain().addLast("logger", new LoggingFilter());
			acceptor.setHandler(baseServerHandler);
			acceptor.bind(new InetSocketAddress(port));

			System.out.println("Server now listening on port " + port);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}