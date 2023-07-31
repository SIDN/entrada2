/*
 * ENTRADA, a big data platform for network data analytics
 *
 * Copyright (C) 2016 SIDN [https://www.sidn.nl]
 * 
 * This file is part of ENTRADA.
 * 
 * ENTRADA is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * ENTRADA is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with ENTRADA. If not, see
 * [<http://www.gnu.org/licenses/].
 *
 */
package nl.sidn.entrada2.worker.service.emrich.geoip;


import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.maxmind.db.CHMCache;
import com.maxmind.db.Reader.FileMode;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CountryResponse;
import jakarta.annotation.PostConstruct;
import lombok.extern.log4j.Log4j2;

/**
 * Utility class to lookup IP adress information such as country and asn. Uses the maxmind database
 */
@Log4j2
@Component
public class GeoIPService {

  // free dbs
  private static final String FILENAME_GEOLITE_COUNTRY = "GeoLite2-Country.mmdb";
  private static final String FILENAME_GEOLITE_ASN = "GeoLite2-ASN.mmdb";
  // paid dbs
  private static final String FILENAME_GEOIP2_COUNTRY = "GeoIP2-Country.mmdb";
  private static final String FILENAME_GEOIP2_ASN = "GeoIP2-ISP.mmdb";

  private static final int DEFAULT_CACHE_SIZE = 1024 * 1000;

  @Value("${maxmind.max-age:24}")
  private int maxAge;
  @Value("${maxmind.country.free}")
  private String urlCountryDb;
  @Value("${maxmind.asn.free}")
  private String urlAsnDb;
  @Value("${maxmind.country.paid}")
  private String urlCountryDbPaid;
  @Value("${maxmind.asn.paid}")
  private String urlAsnDbPaid;

  @Value("${maxmind.license.free}")
  private String licenseKeyFree;

  @Value("${maxmind.license.paid}")
  private String licenseKeyPaid;

  private DatabaseReader geoReader;
  private DatabaseReader asnReader;

  private boolean usePaidVersion;
  private boolean geoDbInitialised;

  private LocalDateTime lastUpdateTime = null;

  @PostConstruct
  public void initialize() {
    loadDatabases();
  }

  public void loadDatabases() {

    if (StringUtils.isBlank(licenseKeyFree) && StringUtils.isBlank(licenseKeyPaid)) {
      throw new RuntimeException(
          "No valid Maxmind license key found, provide key for either the free of paid license.");
    }

    usePaidVersion = StringUtils.isNotBlank(licenseKeyPaid);

    geoDbInitialised = false;

    boolean updateOk = false;

    String url = urlCountryDb + licenseKeyFree;
    if (usePaidVersion) {
      log.info("Download paid Maxmind country database");
      url = urlCountryDbPaid + licenseKeyPaid;
    }
    
    if(log.isDebugEnabled()) {
      log.debug("Download URL: " + url);
    }

    try ( TarArchiveInputStream tar =  new TarArchiveInputStream(new GZIPInputStream(new URL(url).openStream()))) {
      
      TarArchiveEntry entry = tar.getNextTarEntry();
      while (entry != null) {
        if(StringUtils.contains(entry.getName(), countryFile())) {
          
          geoReader = new DatabaseReader.Builder(tar)
              .withCache(new CHMCache(DEFAULT_CACHE_SIZE))
              .fileMode(FileMode.MEMORY)
              .build();

          updateOk = true;
        }
        entry = tar.getNextTarEntry();
      }       
      
      
    } catch (IOException e) {
      throw new RuntimeException("Error initializing Maxmind GEO/ASN database", e);
    }
    
    url = urlAsnDb + licenseKeyFree;
    if (usePaidVersion) {
      log.info("Download paid Maxmind ISP database");
      url = urlAsnDbPaid + licenseKeyPaid;
    }
    
    if(log.isDebugEnabled()) {
      log.debug("Download URL: " + url);
    }
    
    try( TarArchiveInputStream tar =  new TarArchiveInputStream(new GZIPInputStream(new URL(url).openStream()))) {
      
      TarArchiveEntry entry = tar.getNextTarEntry();
      while (entry != null) {
        if(StringUtils.contains(entry.getName(), asnFile())) {
          
          asnReader = new DatabaseReader.Builder(tar)
              .withCache(new CHMCache(DEFAULT_CACHE_SIZE))
              .fileMode(FileMode.MEMORY)
              .build();

          if (updateOk) {
            // both db updated ok
            geoDbInitialised = true;
            lastUpdateTime = LocalDateTime.now();
          }
        }
        entry = tar.getNextTarEntry();
      }       

    } catch (IOException e) {
      throw new RuntimeException("Error initializing Maxmind GEO/ASN database", e);
    }

  }

  private String countryFile() {
    return usePaidVersion ? FILENAME_GEOIP2_COUNTRY : FILENAME_GEOLITE_COUNTRY;
  }

  private String asnFile() {
    return usePaidVersion ? FILENAME_GEOIP2_ASN : FILENAME_GEOLITE_ASN;
  }

  /**
   * Check if the database should be updated
   * 
   * @param database named of database
   * @return true if database file does not exist or is too old
   */
  public boolean shouldUpdate() {
    return LocalDateTime.now().minusHours(maxAge).isAfter(lastUpdateTime);
  }
  
  public Optional<CountryResponse> lookupCountry(InetAddress ip) {
    try {
      return geoReader.tryCountry(ip);
    } catch (Exception e) {
      log.error("Maxmind lookup error for: {}", ip, e);
    }
    return Optional.empty();
  }


  public Optional<? extends AsnResponse> lookupASN(InetAddress ip) {
    try {
      if (usePaidVersion) {
        // paid version returns IspResponse
        return asnReader.tryIsp(ip);
      }

      // use free version
      return asnReader.tryAsn(ip);

    } catch (Exception e) {
      log.error("Maxmind error for IP: {}", ip, e);
    }

    return Optional.empty();
  }


  public DatabaseReader getGeoReader() {
    return geoReader;
  }

  public DatabaseReader getAsnReader() {
    return asnReader;
  }

  public boolean isGeoDbInitialised() {
    return geoDbInitialised;
  }

}
