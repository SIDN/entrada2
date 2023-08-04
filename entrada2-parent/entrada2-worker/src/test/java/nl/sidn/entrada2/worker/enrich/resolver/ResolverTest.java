package nl.sidn.entrada2.worker.enrich.resolver;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import nl.sidn.entrada2.worker.service.emrich.resolver.DnsResolverCheck;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class ResolverTest {
  
  @Autowired 
  private List<DnsResolverCheck> checks;
  
  
  @Test
  public void testLoadResolverChecks(){
        
    assertNotNull(checks);
    assertTrue(checks.size() == 4);

  }

}
