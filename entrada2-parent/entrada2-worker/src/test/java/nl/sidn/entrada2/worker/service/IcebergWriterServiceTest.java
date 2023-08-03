package nl.sidn.entrada2.worker.service;


import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class IcebergWriterServiceTest {

  @Autowired
  private RESTCatalog catalog;
  @Autowired
  private  Schema schema;

  @Test
  public void testCreateSchema() throws Exception {
    //Schema schema = writer.createSchema();
    assertNotNull(schema);
  }


  @Test
  public void testCreateCatalog() throws Exception {
   // RESTCatalog catalog = writer.createCatalog();
    assertNotNull(catalog);   
    assertFalse(catalog.namespaceExists(Namespace.of("entrada")));
  }

}
