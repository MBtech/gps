package gps.node.master.monitoring;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.demux.MessageDecoder;
import org.apache.mina.filter.codec.demux.MessageDecoderAdapter;
import org.apache.mina.filter.codec.demux.MessageDecoderResult;

/**
 * A {@link MessageDecoder} that decodes {@link HttpRequestMessage}.
 */
public class HttpRequestDecoder extends MessageDecoderAdapter {

	private static Logger logger = Logger.getLogger(HttpRequestDecoder.class);

	private static final byte[] CONTENT_LENGTH = new String("Content-Length:").getBytes();

	private final CharsetDecoder decoder = Charset.defaultCharset().newDecoder();

	@Override
	public MessageDecoderResult decodable(IoSession session, IoBuffer in) {
		// Return NEED_DATA if the whole header is not read yet.
		try {
			return messageComplete(in) ? MessageDecoderResult.OK : MessageDecoderResult.NEED_DATA;
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return MessageDecoderResult.NOT_OK;
	}

	@Override
	public MessageDecoderResult decode(IoSession session, IoBuffer in, ProtocolDecoderOutput out)
		throws Exception {
		// Try to decode body
		HttpRequestMessage m = decodeBody(in);

		// Return NEED_DATA if the body is not fully read.
		if (m == null) {
			return MessageDecoderResult.NEED_DATA;
		}

		out.write(m);

		return MessageDecoderResult.OK;
	}

	private boolean messageComplete(IoBuffer in) throws Exception {
		int last = in.remaining() - 1;
		if (in.remaining() < 4) {
			return false;
		}

		// to speed up things we check if the Http request is a GET or POST
		if (in.get(0) == (byte) 'G' && in.get(1) == (byte) 'E' && in.get(2) == (byte) 'T') {
			// Http GET request therefore the last 4 bytes should be 0x0D 0x0A 0x0D 0x0A
			return in.get(last) == (byte) 0x0A && in.get(last - 1) == (byte) 0x0D
				&& in.get(last - 2) == (byte) 0x0A && in.get(last - 3) == (byte) 0x0D;
		} else if (in.get(0) == (byte) 'P' && in.get(1) == (byte) 'O' && in.get(2) == (byte) 'S'
			&& in.get(3) == (byte) 'T') {
			// Http POST request
			// first the position of the 0x0D 0x0A 0x0D 0x0A bytes
			int eoh = -1;
			for (int i = last; i > 2; i--) {
				if (in.get(i) == (byte) 0x0A && in.get(i - 1) == (byte) 0x0D
					&& in.get(i - 2) == (byte) 0x0A && in.get(i - 3) == (byte) 0x0D) {
					eoh = i + 1;
					break;
				}
			}
			if (eoh == -1) {
				return false;
			}
			for (int i = 0; i < last; i++) {
				boolean found = false;
				for (int j = 0; j < CONTENT_LENGTH.length; j++) {
					if (in.get(i + j) != CONTENT_LENGTH[j]) {
						found = false;
						break;
					}
					found = true;
				}
				if (found) {
					// retrieve value from this position till next 0x0D 0x0A
					StringBuilder contentLength = new StringBuilder();
					for (int j = i + CONTENT_LENGTH.length; j < last; j++) {
						if (in.get(j) == 0x0D) {
							break;
						}
						contentLength.append(new String(new byte[] { in.get(j) }));
					}
					// if content-length worth of data has been received then the message is
					// complete
					return Integer.parseInt(contentLength.toString().trim()) + eoh == in
						.remaining();
				}
			}
		}

		// the message is not complete and we need more data
		return false;
	}

	private HttpRequestMessage decodeBody(IoBuffer in) throws IOException {
		try {
			return parseRequest(new StringReader(in.getString(decoder)));
		} catch (CharacterCodingException e) {
			e.printStackTrace();
			throw e;
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
	}

	private HttpRequestMessage parseRequest(Reader is) throws IOException {
		logger.info("Starting to parse the http request...");
		BufferedReader rdr = new BufferedReader(is);

		// Get request URL.
		String line = rdr.readLine();
		logger.info("Request URL: " + line);
		String[] url = line.split(" ");
		if (url.length < 1) {
			String errorString = "HTTP Request: " + line + "is not valid";
			logger.error("Request does not have relative index.");
			throw new MalformedURLException(errorString);
		}
		if (!url[0].equalsIgnoreCase("GET")) {
			throw new MalformedURLException("Only GET requests are supported.");
		}

		String relativeURLWithArgs = url[1];
		String relativeURL = null;
		Map<String, String> argsMap = new HashMap<String, String>();
		int idx = url[1].indexOf('?');
		if (idx != -1) {
			relativeURL = relativeURLWithArgs.substring(1, idx);
			line = relativeURLWithArgs.substring(idx + 1);
		} else {
			relativeURL = relativeURLWithArgs.substring(1);
			line = null;
		}
		if (line != null) {
			String[] match = line.split("\\&");
			for (String element : match) {
				String[] tokens = element.split("=");
				if (2 == tokens.length) {
					argsMap.put(tokens[0], tokens[1]);
				} else {
					logger.error("Malformed argument: " + element + ". Skipping...");
				}
			}
		}
		return new HttpRequestMessage(relativeURL, argsMap);
	}
}