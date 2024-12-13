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
package nl.sidn.entrada2.load;

import org.apache.commons.lang3.StringUtils;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RequestCacheKey {

	private final int id;
	private final String qname;
	private final String src;
	private final int srcPort;

	@Override
	public int hashCode() {
		// us the client port+id as hashcode, this may lead to collisions for bad clients using
		// some port and id for all queries and maybe for ddos situations?
		// this way we can an acceptable distibuted hash very fast
		return srcPort + id;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		
		RequestCacheKey other = (RequestCacheKey) obj;
		
		if (id != other.id)
			return false;
		
		if (srcPort != other.srcPort)
			return false;

		if (!StringUtils.equals(qname, other.qname))
			return false;

		if (!StringUtils.equals(src, other.src))
			return false;
		
		return true;
	}

}
