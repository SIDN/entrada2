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
package nl.sidn.entrada2.worker.service.enrich.geoip;


import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import com.maxmind.db.CHMCache;
import com.maxmind.db.Reader.FileMode;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CountryResponse;
import feign.Response;
import jakarta.annotation.PostConstruct;
import lombok.extern.log4j.Log4j2;
import nl.sidn.entrada2.worker.service.enrich.geoip.AbstractMaxmind.DB_TYPE;

/**
 * Utility class to lookup IP adress information such as country and asn. Uses the maxmind database
 */
@Log4j2
@Component
//@ConditionalOnProperty( name = "entrada.mode", havingValue = "worker")
public class GeoIPService extends AbstractMaxmind {

  private DatabaseReader geoReader;
  private DatabaseReader asnReader;

  private boolean usePaidVersion;
  private LocalDateTime lastUpdateTime = null;

  private List<Pair<String, LocalDateTime>> s3Databases;
  
  @Value("${entrada.mode}")
  protected String mode;

  @PostConstruct
  public void load() {
    
    if(StringUtils.equalsIgnoreCase(mode, "controller")) {
      // only 1 controller will download data and save to s3
      downloadWhenRequired();
    }else {
      //  only load data when running as worker
      geoReader = loadDatabase(DB_TYPE.COUNTRY).get();
      asnReader = loadDatabase(DB_TYPE.ASN).get();
      
      usePaidVersion = isPaidVersion();
      lastUpdateTime = LocalDateTime.now();
    }
  }
  
  public void downloadWhenRequired() {
  List<Pair<String, LocalDateTime>> dbs = s3FileService.ls(bucket, directory);
    
    for(Pair<String, LocalDateTime> db: dbs) {
      if(lastUpdateTime == null || db.getValue().isAfter(lastUpdateTime)) {
        // new db avail on s3,
        download();
        return;
      }
    }
    
    log.info("No new databases found on s3");
  }
  

  private Optional<DatabaseReader> loadDatabase(DB_TYPE dbType) {
    
    log.info("Load Maxmind database: {}", dbType);
    
    String key = directory + "/" + ((dbType == DB_TYPE.COUNTRY) ? countryFile() : asnFile());

    Optional<InputStream> ois = s3FileService.read(bucket, key);
    if(ois.isPresent()) {
      
      try {
        return Optional.of(new DatabaseReader.Builder(ois.get())
            .withCache(new CHMCache(DEFAULT_CACHE_SIZE))
            .fileMode(FileMode.MEMORY)
            .build());
      } catch (IOException e) {
        throw new RuntimeException("Cannot read Maxmind database", e);
      }
      
    }
    return Optional.empty();
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
  
  //@PostConstruct
  public void download() {

    if (StringUtils.isBlank(licenseKeyFree) && StringUtils.isBlank(licenseKeyPaid)) {
      throw new RuntimeException(
          "No valid Maxmind license key found, provide key for either the free of paid license.");
    }

    s3Databases = s3FileService.ls(bucket, directory);
    String license = StringUtils.isNotBlank(licenseKeyPaid) ? licenseKeyPaid : licenseKeyFree;
    
    String dbName = StringUtils.isNotBlank(licenseKeyPaid) ? countryDbPaid : countryDb;
    downloadDatabase(DB_TYPE.COUNTRY, dbName, license);

    dbName = StringUtils.isNotBlank(licenseKeyPaid) ? asnDbPaid : asnDb;
    downloadDatabase(DB_TYPE.ASN, dbName, license);

  }


  private void downloadDatabase(DB_TYPE type, String db, String license) {

    String filename = (type == DB_TYPE.COUNTRY) ? countryFile() : asnFile();

    ResponseEntity<Void> resPeek = mmClient.peek(db, license);


    LocalDateTime lastModifiedRemote = ZonedDateTime.parse(
        resPeek.getHeaders().getFirst("last-modified"),
        DateTimeFormatter.RFC_1123_DATE_TIME).toLocalDateTime();


    if( !s3Databases.stream().anyMatch(p -> StringUtils.containsIgnoreCase(p.getKey(), filename)) ||
        s3Databases.stream().anyMatch(p -> StringUtils.containsIgnoreCase(p.getKey(), filename) &&
        lastModifiedRemote.isAfter(p.getValue()))) {
      
      log.info("Downloading Maxmind database: {}", db);
      
      // no file found or old file, download new
      Response response = mmClient.getDatabase(db, license);

      try (TarArchiveInputStream tar =
          new TarArchiveInputStream(new GZIPInputStream(response.body().asInputStream()))) {

        TarArchiveEntry entry = tar.getNextTarEntry();
        while (entry != null) {
          if (StringUtils.contains(entry.getName(), filename)) {

            s3FileService.write(tar, bucket, directory + "/" + filename);
            return;
          }
          entry = tar.getNextTarEntry();
        }
      } catch (IOException e) {
        throw new RuntimeException("Error initializing Maxmind GEO/ASN database", e);
      } 
    }else {
      log.info("No new {} database found online", type);
    }
  }


}
