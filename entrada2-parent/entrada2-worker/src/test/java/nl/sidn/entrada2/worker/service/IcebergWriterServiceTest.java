package nl.sidn.entrada2.worker.service;


import static org.junit.jupiter.api.Assertions.assertNotNull;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

public class IcebergWriterServiceTest {

  @Test
  public void testLoadSchemaFromAvro()throws Exception {

    //InputStream is = new ClassPathResource("avro/dns-query.avsc", getClass()).getInputStream();
    InputStream is = getClass().getResourceAsStream("/avro/dns-query.avsc");
    assertNotNull(is);

    Schema avroSchema = new Schema.Parser().parse(is);
    assertNotNull(avroSchema);

    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
    assertNotNull(icebergSchema);

  }

}
