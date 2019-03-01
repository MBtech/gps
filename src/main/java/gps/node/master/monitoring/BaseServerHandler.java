package gps.node.master.monitoring;

import org.apache.log4j.Logger;
import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;

public abstract class BaseServerHandler extends IoHandlerAdapter {
	
	@Override
	public void sessionOpened(IoSession session) {
		session.getConfig().setIdleTime(IdleStatus.BOTH_IDLE, 60);
	}

	protected HttpResponseMessage getErrorResponsePage(String errorMessage) {
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_NOT_FOUND);
		return response;
	}

	@Override
	public void sessionIdle(IoSession session, IdleStatus status) {
		// TODO(semih): Log this event
		session.close(true /* immediately */);
	}

	@Override
	public void exceptionCaught(IoSession session, Throwable cause) {
		cause.printStackTrace();
		getLogger().error(cause.getMessage());
		session.close(true /* immediately */);
	}

	@Override
	public void messageReceived(IoSession session, Object message) {
		HttpRequestMessage request = (HttpRequestMessage) message;
		HttpResponseMessage response = null;
		String relativeURL = request.getRelativeURL();
		response = handleMessageReceived(request, relativeURL);
		if (response != null) {
			session.write(response).addListener(IoFutureListener.CLOSE);
		}
	}

	protected abstract HttpResponseMessage handleMessageReceived(HttpRequestMessage request,
		String relativeURL);

	public abstract Logger getLogger();
}
