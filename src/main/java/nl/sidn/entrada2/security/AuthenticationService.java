package nl.sidn.entrada2.security;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.stereotype.Service;
import jakarta.servlet.http.HttpServletRequest;
import nl.sidn.entrada2.data.AuthTokenRepository;
import nl.sidn.entrada2.data.model.AuthToken;

@Service
@Profile("controller")
public class AuthenticationService {

  private static final String AUTH_TOKEN_HEADER_NAME = "X-API-KEY";

  @Value("${entrada.security.token.admin}")
  private String adminToken;
  
  @Value("${entrada.security.token.user}")
  private String userToken;

  @Autowired
  private AuthTokenRepository authRepository;


  public Authentication getAuthentication(HttpServletRequest request) {
    String apiKey = request.getHeader(AUTH_TOKEN_HEADER_NAME);
    if (StringUtils.isNotBlank(apiKey)) {

      if (StringUtils.equals(apiKey, adminToken)) {
        return new ApiKeyAuthentication(apiKey, AuthorityUtils.createAuthorityList("ROLE_ADMIN"));
      }
      
      if (StringUtils.equals(apiKey, userToken)) {
        return new ApiKeyAuthentication(apiKey, AuthorityUtils.createAuthorityList("ROLE_USER"));
      }

      Optional<AuthToken> optAt = authRepository.findByToken(apiKey);
      if (optAt.isEmpty() || !StringUtils.equals(apiKey, optAt.get().getToken())) {
        throw new BadCredentialsException("Invalid API Key");
      }
    }

    return new ApiKeyAuthentication(apiKey, AuthorityUtils.createAuthorityList("ROLE_USER"));
  }
  
  
  public String createToken(String name) {
    
    AuthToken token = AuthToken.builder()
        .name(name)
        .token( RandomStringUtils.random(32, true, true))
        .created(LocalDateTime.now())
        .enabled(true)
        .build();
    
    token = authRepository.save(token);
    return token.getToken();
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
