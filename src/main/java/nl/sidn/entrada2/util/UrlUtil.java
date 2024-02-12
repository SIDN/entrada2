package nl.sidn.entrada2.util;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.net.URLCodec;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class UrlUtil {
	
	private static final URLCodec urlCodec = new URLCodec();
	
	private UrlUtil() {}
	
	public static String decode(String url) {
		try {
			return urlCodec.decode(url);
		} catch (DecoderException e) {
			log.error("Error decoding url: {}", url);
		}
		
		return "";
	}

}
