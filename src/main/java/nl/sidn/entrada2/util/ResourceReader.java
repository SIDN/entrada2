package nl.sidn.entrada2.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.springframework.core.io.Resource;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.ResourceUtils;

public final class ResourceReader {

	private ResourceReader() {
	}

	public static String resourceToString(Resource resource) {
		try (Reader reader = new InputStreamReader(resource.getInputStream(),  StandardCharsets.UTF_8)) {
			return FileCopyUtils.copyToString(reader);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
	
	 public static String readFileToString(String path) {
	        try {
				return FileUtils.readFileToString(ResourceUtils.getFile(path), StandardCharsets.UTF_8);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
	    }

}
