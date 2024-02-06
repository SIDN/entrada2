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
package nl.sidn.entrada2.service.enrich.geoip;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.time.Instant;
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
import nl.sidn.entrada2.util.TimeUtil;

/**
 * Utility class to lookup IP adress information such as country and asn. Uses
 * the maxmind database
 */
@Log4j2
@Component
public class GeoIPService extends AbstractMaxmind {

	@Value("${spring.cloud.kubernetes.enabled}")
	private boolean k8sEnabled;

	private DatabaseReader geoReader;
	private DatabaseReader asnReader;

	private boolean usePaidVersion;
	private Instant lastUpdateTime = null;

	private List<Pair<String, Instant>> s3Databases;

	private boolean needUpdate = true;

	public void update() {
		// only set update flag to to, let processing thread do the actual update
		// action to prevent thread conflicts
		needUpdate = true;
	}

	@PostConstruct
	public void init() {

		if (!k8sEnabled) {
			downloadWhenRequired();
		}
	}

	public void load() {

		// only load data from s3 when running as worker
		while (geoReader == null) {
			log.info("Try loading GEO database");
			geoReader = loadDatabase(DB_TYPE.COUNTRY).get();
			TimeUtil.sleep(5000);
		}

		while (asnReader == null) {
			log.info("Try loading ASN database");
			asnReader = loadDatabase(DB_TYPE.ASN).get();
			TimeUtil.sleep(5000);
		}

		usePaidVersion = isPaidVersion();
		lastUpdateTime = Instant.now();
		// }
	}

	public void downloadWhenRequired() {
		s3Databases = s3FileService.ls(bucket, directory);

		if(s3Databases.isEmpty()) {
			download();
			return;
		}
		
		for (Pair<String, Instant> db : s3Databases) {
			if (lastUpdateTime == null || db.getValue().isAfter(lastUpdateTime)) {
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
		if (ois.isPresent()) {

			try {
				return Optional.of(new DatabaseReader.Builder(ois.get()).withCache(new CHMCache(DEFAULT_CACHE_SIZE))
						.fileMode(FileMode.MEMORY).build());
			} catch (IOException e) {
				throw new RuntimeException("Cannot read Maxmind database", e);
			}

		}
		return Optional.empty();
	}

	public Optional<CountryResponse> lookupCountry(InetAddress ip) {

		if (needUpdate) {
			load();
			needUpdate = false;
		}

		try {
			return geoReader.tryCountry(ip);
		} catch (Exception e) {
			log.error("Maxmind lookup error for: {}", ip, e);
		}
		return Optional.empty();
	}

	public Optional<? extends AsnResponse> lookupASN(InetAddress ip) {

		if (needUpdate) {
			load();
			needUpdate = false;
		}

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

	private void download() {

		if (StringUtils.isBlank(licenseKeyFree) && StringUtils.isBlank(licenseKeyPaid)) {
			throw new RuntimeException(
					"No valid Maxmind license key found, provide key for either the free of paid license.");
		}

		String license = StringUtils.isNotBlank(licenseKeyPaid) ? licenseKeyPaid : licenseKeyFree;
		String dbName = StringUtils.isNotBlank(licenseKeyPaid) ? countryDbPaid : countryDb;
		downloadDatabase(DB_TYPE.COUNTRY, dbName, license);

		dbName = StringUtils.isNotBlank(licenseKeyPaid) ? asnDbPaid : asnDb;
		downloadDatabase(DB_TYPE.ASN, dbName, license);

	}

	private void downloadDatabase(DB_TYPE type, String db, String license) {

		String filename = (type == DB_TYPE.COUNTRY) ? countryFile() : asnFile();

		ResponseEntity<Void> resPeek;
		try {
			resPeek = mmClient.peek(db, license);
		} catch (Exception e) {
			log.error("Error while checking for new online database", e);
			return;
		}

		ZonedDateTime lastModifiedRemote = ZonedDateTime
				.parse(resPeek.getHeaders().getFirst("last-modified"), DateTimeFormatter.RFC_1123_DATE_TIME);

		Instant instant = lastModifiedRemote.toInstant();

		if (!s3Databases.stream().anyMatch(p -> StringUtils.containsIgnoreCase(p.getKey(), filename))
				|| s3Databases.stream().anyMatch(
						p -> StringUtils.containsIgnoreCase(p.getKey(), filename) && instant.isAfter(p.getValue()))) {

			log.info("Downloading Maxmind database: {}", db);

			// no file found or old file, download new
			Response response = mmClient.getDatabase(db, license);

			try (TarArchiveInputStream tar = new TarArchiveInputStream(
					new GZIPInputStream(response.body().asInputStream()))) {

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
		} else {
			log.info("No new {} database found online", type);
		}
	}

}
