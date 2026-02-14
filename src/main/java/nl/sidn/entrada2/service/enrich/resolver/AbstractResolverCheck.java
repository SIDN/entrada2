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
package nl.sidn.entrada2.service.enrich.resolver;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.net.util.SubnetUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import nl.sidn.entrada2.service.S3Service;

@Log4j2
@Getter
@Setter
public abstract class AbstractResolverCheck implements DnsResolverCheck {

	@Value("${resolver.match.cache.size:10000}")
	private int maxMatchCacheSize;

	@Autowired
	protected S3Service s3;

	@Value("${entrada.s3.bucket}")
	protected String bucket;

	@Value("${entrada.s3.reference-dir}")
	protected String directory;
	
	private BloomFilter<String> ipv4Filter;
	
	private SubnetChecker subnetChecker = new SubnetChecker();

	private boolean needToReload = true;

	public void update() {
		needToReload = true;
	}

	private void createIpV4BloomFilter(List<String> subnets) {
		Set<String> ipv4Set = new HashSet<>();
		if (!subnets.isEmpty()) {

			for (String sn : subnets) {
				if (sn.contains(".")) {
					String cidr = StringUtils.substringAfter(sn, "/");
					if (NumberUtils.isCreatable(cidr)) {
						SubnetUtils utils = new SubnetUtils(sn);
						for (String ip : utils.getInfo().getAllAddresses()) {
							ipv4Set.add(ip);
						}
					} else {
						log.info("Not adding invalid subnet {} to IPv4 bloomfilter", sn);
					}
				}
			}
		}

		ipv4Filter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.US_ASCII), ipv4Set.size(), 0.01);

		for (String addr : ipv4Set) {
			ipv4Filter.put(addr);
		}

		log.info("Created IPv4 filter table with size: {}", ipv4Set.size());
	}

	@Override
	public List<String> loadFromFile() {

		Optional<String> os = s3.readObjectAsString(bucket, directory + "/" + getFilename());
		if (os.isPresent()) {
			return Splitter.on("\n").splitToList(os.get());

		}

		return Collections.emptyList();

	}

	@Override
	public void download() {
		List<String> subnets = fetch();
		s3.write(bucket, directory + "/" + getFilename(), Joiner.on("\n").join(subnets));
	}

	private void loadData() {
		List<String> lines = loadFromFile();

		createIpV4BloomFilter(lines);

		subnetChecker.clear();

		lines.stream().filter(s -> s.contains(".")).filter(Objects::nonNull)
				.forEach(s -> subnetChecker.precomputeV4Mask(s, 4));

		lines.stream().filter(s -> s.contains(":")).filter(Objects::nonNull)
		.forEach(s -> subnetChecker.precomputeV4Mask(s, 6));

		log.info("Loaded {} resolver addresses for {}", getMatcherCount(), getName());
	}

	private boolean isIpv4(String address) {
		return address.indexOf('.') != -1;
	}

	private boolean isIpv4InFilter(String address) {
		return ipv4Filter.mightContain(address);
	}

	@Override
	public boolean match(String address, InetAddress inetAddress) {

		if (needToReload) {
			// reload inline to prevent concurrent mopdification exception
			loadData();
			needToReload = false;
		}
		
		if (isIpv4(address)) {
			// do v4 check only
			if (isIpv4InFilter(address)) {
				// the address may be in the cidr list
				return subnetChecker.match(address);
			}

			// the v4 addr is for sure not in the v4 cidr list
			return false;

		}
		
		// check v6
		return subnetChecker.match(address);

	}

	private int getMatcherCount() {
		return subnetChecker.size();
	}

}
