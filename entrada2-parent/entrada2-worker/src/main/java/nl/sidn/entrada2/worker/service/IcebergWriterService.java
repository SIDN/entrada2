package nl.sidn.entrada2.worker.service;

import java.io.IOException;
import java.io.InputStream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.springframework.stereotype.Service;

@Service
public class IcebergWriterService {
  
  private  Schema schema;
  

  private Schema createSchema() {

    //InputStream is = new ClassPathResource("avro/dns-query.avsc", getClass()).getInputStream();
    InputStream is = getClass().getResourceAsStream("/avro/dns-query.avsc");

    try {
      org.apache.avro.Schema  avroSchema = new org.apache.avro.Schema.Parser().parse(is);
      return AvroSchemaUtil.toIceberg(avroSchema);
    } catch (IOException e) {
      throw new RuntimeException("Error creating Avro schema", e);
    }  
  }

  public void write() {
    if(schema == null) {
      schema = createSchema();
    }
  }
  
}
