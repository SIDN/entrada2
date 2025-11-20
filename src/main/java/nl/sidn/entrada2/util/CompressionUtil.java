package nl.sidn.entrada2.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.lang3.StringUtils;
import org.tukaani.xz.XZInputStream;

public class CompressionUtil {
	
	private CompressionUtil() {
	}

	/**
	 * wraps the inputstream with a decompressor based on a filename ending
	 *
	 * @param in       The input stream to wrap with a decompressor
	 * @param filename The filename from which we guess the correct decompressor
	 * @param bufSize  size of the read buffer to use, in bytes
	 * @return the compressor stream wrapped around the inputstream. If no
	 *         decompressor is found, returns the inputstream wrapped in a
	 *         BufferedInputStream
	 * @throws IOException when stream cannot be created
	 */
	public static InputStream getDecompressorStreamWrapper(InputStream in, int bufSize, String filename)
			throws IOException {

		if (StringUtils.endsWithIgnoreCase(filename, ".pcap")) {
			return wrap(in, bufSize);
		} else if (StringUtils.endsWithIgnoreCase(filename, ".gz")) {
			return new GzipCompressorInputStream(wrap(in, bufSize));
		} else if (StringUtils.endsWithIgnoreCase(filename, ".xz")) {
			return new XZInputStream(wrap(in, bufSize));
		} else if (StringUtils.endsWithIgnoreCase(filename, ".bz2")) {
			return new BZip2CompressorInputStream(wrap(in, bufSize), true);
		}

		// unkown file type
		throw new RuntimeException("Could not open file with unknown extension: " + filename);
	}
	
	public static boolean isSupportedFormat(String filename) {
		
		return switch (StringUtils.substringAfterLast(filename, ".")) {
		  case "pcap", "gz", "xz", "bz2" -> true;
		  default -> false;
		};
	}

	private static InputStream wrap(InputStream in, int bufSize) {
		if (!(in instanceof BufferedInputStream)) {
			return new BufferedInputStream(in, bufSize);
		}

		return in;
	}

	public static byte[] compress(byte[] body) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(baos)) {
			gzipOutputStream.write(body);
		}
		return baos.toByteArray();
	}
	
	public static byte[] decompress(byte[] body) throws IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(body);
		try (GZIPInputStream gzipInputStream = new GZIPInputStream(bais)) {
			return gzipInputStream.readAllBytes();
		}
	}

}
