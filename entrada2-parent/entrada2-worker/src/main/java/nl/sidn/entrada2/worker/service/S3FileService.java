package nl.sidn.entrada2.worker.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.mediastoredata.model.GetObjectRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import lombok.extern.slf4j.Slf4j;
import nl.sidn.entrada.data.Work;

@Service
@Slf4j
public class S3FileService {
  
  @Autowired
  private AmazonS3 amazonS3;
  
  public Optional<InputStream> read(String bucket, String key) {
    try {
      S3Object obj = amazonS3.getObject(bucket, key);
     // log.info("got bytes: " + obj.getObjectContent().available());
      return Optional.of(obj.getObjectContent());
    } catch (SdkClientException e) {
      log.error("Errow file getting {} from bucket {}", key, bucket, e);
      return Optional.empty();
    }
    
   // return Optional.empty();
  }

}
