package nl.sidn.entrada2.service.enrich.geoip;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import nl.sidn.entrada2.service.S3Service;

public abstract class AbstractMaxmind {
  
  @Value("${maxmind.max-age:24}")
  protected int maxAge;
  @Value("${maxmind.country.free}")
  protected String countryDb;
  @Value("${maxmind.asn.free}")
  protected String asnDb;
  @Value("${maxmind.country.paid}")
  protected String countryDbPaid;
  @Value("${maxmind.asn.paid}")
  protected String asnDbPaid;

  @Value("${maxmind.license.free}")
  protected String licenseKeyFree;

  @Value("${maxmind.license.paid}")
  protected String licenseKeyPaid;

  @Autowired
  protected S3Service s3FileService;

  @Autowired
  protected MaxmindClient mmClient;

  @Value("${entrada.s3.bucket}")
  protected String bucket;

  @Value("${entrada.directory.reference}")
  protected String directory;
  
  // free dbs
  protected static final String FILENAME_GEOLITE_COUNTRY = "GeoLite2-Country.mmdb";
  protected static final String FILENAME_GEOLITE_ASN = "GeoLite2-ASN.mmdb";
  // paid dbs
  protected static final String FILENAME_GEOIP2_COUNTRY = "GeoIP2-Country.mmdb";
  protected static final String FILENAME_GEOIP2_ASN = "GeoIP2-ISP.mmdb";
  
  protected static final int DEFAULT_CACHE_SIZE = 1024 * 1000;

  protected enum DB_TYPE {
    COUNTRY, ASN
  };

  public String countryFile() {
    return StringUtils.isNotBlank(licenseKeyPaid) ? FILENAME_GEOIP2_COUNTRY
        : FILENAME_GEOLITE_COUNTRY;
  }

  public String asnFile() {
    return StringUtils.isNotBlank(licenseKeyPaid) ? FILENAME_GEOIP2_ASN : FILENAME_GEOLITE_ASN;
  }
  
  protected boolean isPaidVersion() {
    return StringUtils.isNotBlank(licenseKeyPaid);
  }
}
