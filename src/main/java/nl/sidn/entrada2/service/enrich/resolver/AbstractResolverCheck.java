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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import nl.sidn.entrada2.service.S3Service;

@Log4j2
@Getter
@Setter
public abstract class AbstractResolverCheck implements DnsResolverCheck {

	private List<FastIpSubnet> matchers4 = new ArrayList<>();
	private List<FastIpSubnet> matchers6 = new ArrayList<>();

	@Value("${resolver.match.cache.size:10000}")
	private int maxMatchCacheSize;

	@Autowired
	protected S3Service s3;

	@Value("${entrada.s3.bucket}")
	protected String bucket;

	@Value("${entrada.directory.reference}")
	protected String directory;
	
	@Value("${spring.cloud.kubernetes.enabled}")
	private boolean k8sEnabled;

	private BloomFilter<String> ipv4Filter;

	private boolean needToReload = true;

//	@PostConstruct
//	public void init() {
//		if (!k8sEnabled) {
//			// only 1 pod will download data and save to s3
//			download();
//		} 
//	}

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

		ipv4Filter = BloomFilter.create(Funnels.stringFunnel(Charsets.US_ASCII), ipv4Set.size(), 0.01);

		for (String addr : ipv4Set) {
			ipv4Filter.put(addr);
		}

		log.info("Created IPv4 filter table with size: {}", ipv4Set.size());
	}

	@Override
	public List<String> loadFromFile() {

		Optional<String> os = s3.readObectAsString(bucket, directory + "/" + getFilename());
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

		matchers4.clear();
		matchers6.clear();

		lines.stream().filter(s -> s.contains(".")).map(this::subnetFor).filter(Objects::nonNull)
				.forEach(s -> matchers4.add(s));

		lines.stream().filter(s -> s.contains(":")).map(this::subnetFor).filter(Objects::nonNull)
				.forEach(s -> matchers6.add(s));

		log.info("Loaded {} resolver addresses", getMatcherCount());
	}

	private FastIpSubnet subnetFor(String address) {
		try {
			return new FastIpSubnet(address);
		} catch (UnknownHostException e) {
			log.error("Cannot create subnet for: {}", address, e);
		}

		return null;
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
				return checkv4(address, inetAddress);
			}

			return false;

		}

		// do v6 check only
		return checkv6(address, inetAddress);
	}

	private boolean checkv4(String address, InetAddress inetAddress) {
		for (FastIpSubnet sn : matchers4) {
			if (sn.contains(inetAddress)) {
				// addToCache(address);
				return true;
			}
		}
		return false;
	}

	private boolean checkv6(String address, InetAddress inetAddress) {
		for (FastIpSubnet sn : matchers6) {
			if (sn.contains(inetAddress)) {
				// addToCache(address);
				return true;
			}
		}
		return false;
	}

	private int getMatcherCount() {
		return matchers4.size() + matchers6.size();
	}

}
