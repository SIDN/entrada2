package nl.sidn.entrada2.load;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.iceberg.data.GenericRecord;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import nl.sidn.entrada2.service.enrich.AddressEnrichment;
import nl.sidn.entrada2.service.enrich.resolver.ResolverEnrichment;

public abstract class AbstractRowBuilder {

  private final static int ENRICHMENT_CACHE_MAX_SIZE = 50000;
  
  @Autowired
  private List<AddressEnrichment> enrichments;

  @Value("${entrada.privacy.enabled:false}")
  protected boolean privacy;
  @Value("#{!T(org.apache.commons.lang3.StringUtils).isBlank('${management.influx.metrics.export.uri:}')}")
  protected boolean metricsEnabled;

  protected Cache<String, List<EnrichmentValue>> enrichmentCache;

  public class EnrichmentValue {

    public EnrichmentValue(String name, String value, boolean resolver) {
      this.name = name;
      this.value = value;
      this.resolver = resolver;
    }

    public String name;
    public String value;
    public boolean resolver;
  }

  public AbstractRowBuilder() {
    enrichmentCache = new Cache2kBuilder<String, List<EnrichmentValue>>() {}
        .entryCapacity(ENRICHMENT_CACHE_MAX_SIZE)
        .build();
  }

  /**
   * Enrich row based on IP address, use both String and InetAddress params tp prevent having to
   * convert between the 2 too many times
   * 
   * @param address
   * @param inetAddress
   * @param prefix
   * @param row
   */
  protected void enrich(String address, InetAddress inetAddress, String prefix,
      GenericRecord record, boolean skipResolvers) {

    List<EnrichmentValue> cached = enrichmentCache.peek(address);
    if (cached != null) {
      for (EnrichmentValue ev : cached) {
        if (skipResolvers && ev.resolver) {
          continue;
        }

        record.setField(prefix + ev.name, ev.value);
      }

      return;
    }

    // not cached, do lookups and cache results

    cached = !skipResolvers ? new ArrayList<>() : null;
    // only perform checks that are required
    for (AddressEnrichment e : enrichments) {
      if (skipResolvers && e instanceof ResolverEnrichment) {
        continue;
      }

      String value = e.match(address, inetAddress);
      if (value != null) {

        record.setField(prefix + e.getColumn(), value);

        if (cached != null) {
          cached.add(new EnrichmentValue(e.getColumn(), value, e instanceof ResolverEnrichment));
        }
      }
    }

    if (cached != null) {
      enrichmentCache.put(address, cached);
    }

    return;
  }

}
