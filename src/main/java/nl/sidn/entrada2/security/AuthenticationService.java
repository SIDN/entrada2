package nl.sidn.entrada2.security;

import java.util.Collection;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.stereotype.Service;

import jakarta.servlet.http.HttpServletRequest;

@Service
public class AuthenticationService {

  private static final String AUTH_TOKEN_HEADER_NAME = "X-API-KEY";

  @Value("${entrada.security.token}")
  private String adminToken;

  public Authentication getAuthentication(HttpServletRequest request) {
    String apiKey = request.getHeader(AUTH_TOKEN_HEADER_NAME);
    if (StringUtils.isNotBlank(apiKey)) {

      if (StringUtils.equals(apiKey, adminToken)) {
        return new ApiKeyAuthentication(apiKey, AuthorityUtils.createAuthorityList("ROLE_ADMIN"));
      }

    }

    throw new BadCredentialsException("Invalid API Key");
  }
  

  public class ApiKeyAuthentication extends AbstractAuthenticationToken {

    private static final long serialVersionUID = -8324199965242715036L;
    private final String apiKey;

    public ApiKeyAuthentication(String apiKey, Collection<? extends GrantedAuthority> authorities) {
      super(authorities);
      this.apiKey = apiKey;
      setAuthenticated(true);
    }

    @Override
    public Object getCredentials() {
      return null;
    }

    @Override
    public Object getPrincipal() {
      return apiKey;
    }
  }
}
