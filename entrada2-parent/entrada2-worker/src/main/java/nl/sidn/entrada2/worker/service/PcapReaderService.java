package nl.sidn.entrada2.worker.service;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada.data.Work;
import nl.sidn.entrada2.worker.util.CompressionUtil;
import nl.sidnlabs.pcap.PcapReader;

@Service
@Slf4j
public class PcapReaderService {

  @Value("${entrada.inputstream.buffer:64}")
  private int bufferSizeConfig;

  @Autowired
  private S3FileService s3FileService;

  public void process(Work work) {

    Optional<InputStream> ois = s3FileService.read(work.getBucket(), work.getKey());
    if (ois.isPresent()) {
      Optional<PcapReader> oreader = createReader(work.getName(), ois.get());
      if (oreader.isPresent()) {
        oreader.get().stream().forEach(p -> {
          System.out.println(p);
        });
      }

    }

  }


  private Optional<PcapReader> createReader(String file, InputStream is) {

    try {
      InputStream decompressor =
          CompressionUtil.getDecompressorStreamWrapper(is, bufferSizeConfig * 1024, file);
      return Optional.of(new PcapReader(new DataInputStream(decompressor), null, true,
          file, false));
    } catch (IOException e) {
      log.error("Error creating pcap reader for: " + file, e);
      try {
        is.close();
      } catch (Exception e2) {
        log.error("Cannot close inputstream, maybe it was not yet opened");
      }
    }
    return Optional.empty();
  }

}
