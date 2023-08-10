package nl.sidn.entrada2.service;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class S3FileService {

  @Autowired
  private AmazonS3 amazonS3;

  public Optional<InputStream> read(String bucket, String key) {
    try {
      S3Object obj = amazonS3.getObject(bucket, key);
      return Optional.of(obj.getObjectContent());
    } catch (SdkClientException e) {
      log.error("Errow file getting {} from bucket {}", key, bucket, e);
      return Optional.empty();
    }
  }

  public Optional<String> readObectAsString(String bucket, String key) {
    
    try (InputStream is = read(bucket, key).get()) {
      return Optional.of(StreamUtils.copyToString(is, StandardCharsets.UTF_8));

    } catch (Exception e) {
      log.error("Errow file getting {} from bucket {}", key, bucket, e);
    }
    return Optional.empty();
  }
  
  public boolean write(String bucket, String key, String content) {
    log.info("Save file: {}", key);

    try {
      amazonS3.putObject(bucket, key, content);
    } catch (Exception e) {
      log.error("Write error", e);
      return false;
    }

    return true;
  }

  public boolean write(InputStream is, String bucket, String key) {
    log.info("Save file: {}", key);

    int fileSize = -1;
    try {

      log.info("Size of file: {}", is.available());

      ObjectMetadata objectMetadata = new ObjectMetadata();
      fileSize = is.available();
      objectMetadata.setContentLength(fileSize);
      amazonS3.putObject(bucket, key, is,
          objectMetadata);

    } catch (Exception e) {
      log.error("Write error", e);
      return false;
    }

    return true;
  }

  public List<Pair<String, LocalDateTime>> ls(String bucket, String key) {
    try {
      ListObjectsV2Result lsResult = amazonS3.listObjectsV2(bucket, key);
      return lsResult.getObjectSummaries().stream()
          .map(o -> Pair.of(o.getKey(), convertDate(o.getLastModified())))
          .collect(Collectors.toList());

    } catch (Exception e) {
      log.error("Read error", e);
    }

    return Collections.emptyList();
  }

  private LocalDateTime convertDate(Date src) {
    return LocalDateTime.ofInstant(src.toInstant(),
        ZoneId.systemDefault());
  }


  public boolean tag(String bucket, String key, String tagName, String tagValue) {
    try {
      ObjectTagging t = new ObjectTagging(Arrays.asList(new Tag(tagName, tagValue)));
      SetObjectTaggingRequest otr = new SetObjectTaggingRequest(bucket, key, t);
      amazonS3.setObjectTagging(otr);

    } catch (Exception e) {
      log.error("Write error", e);
      return false;
    }

    return true;
  }

}
