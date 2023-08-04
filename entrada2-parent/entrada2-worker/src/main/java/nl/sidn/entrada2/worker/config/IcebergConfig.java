package nl.sidn.entrada2.worker.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.rest.RESTCatalog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class IcebergConfig {
  
  @Value("${iceberg.catalog.url}")
  private String catalogUrl;

  @Value("${iceberg.catalog.warehouse}")
  private String catalogWarehouse;
  
  @Value("${iceberg.catalog.endpoint}")
  private String catalogEndpoint;
  
  @Value("${iceberg.catalog.access_key}")
  private String catalogAccessKey;
  
  @Value("${iceberg.catalog.secret_key}")
  private String catalogSecretKey;


  @Bean
  public Schema schema() {
    InputStream is = getClass().getResourceAsStream("/avro/dns-query.avsc");

    try {
      org.apache.avro.Schema  avroSchema = new org.apache.avro.Schema.Parser().parse(is);
      return AvroSchemaUtil.toIceberg(avroSchema);
    } catch (IOException e) {
      throw new RuntimeException("Error creating Avro schema", e);
    }  
  }
  
  
  @Bean
  public RESTCatalog catalog() {
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
    properties.put(CatalogProperties.URI, catalogUrl);
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, catalogWarehouse);
    properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
    properties.put(S3FileIOProperties.ENDPOINT, catalogEndpoint);
    properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, catalogSecretKey);
    properties.put(S3FileIOProperties.ACCESS_KEY_ID, catalogAccessKey);
    properties.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");

    RESTCatalog catalog = new RESTCatalog();

    catalog.initialize("entrada", properties);
    return catalog;
    
}
  
}
