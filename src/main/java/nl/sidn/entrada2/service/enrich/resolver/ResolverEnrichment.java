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
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import nl.sidn.entrada2.load.FieldEnum;
import nl.sidn.entrada2.service.enrich.AddressEnrichment;

@Component
public class ResolverEnrichment implements AddressEnrichment {

  @Autowired
  private List<DnsResolverCheck> resolverChecks;

  public ResolverEnrichment() {
  }

  /**
   * Check if the IP address is linked to a known open resolver operator
   * 
   * @param address IP address to perform lookup with
   * @return Optional with name of resolver operator, empty if no match found
   */
  @Override
  public String match(String address, InetAddress inetAddress) {

    for (DnsResolverCheck check : resolverChecks) {
      if (check.match(address, inetAddress)) {
        String value = check.getName();
        return value;
      }
    }

    return null;
  }


  @Override
  public String getColumn() {
    return FieldEnum.dns_pub_resolver.name();
  }


}
