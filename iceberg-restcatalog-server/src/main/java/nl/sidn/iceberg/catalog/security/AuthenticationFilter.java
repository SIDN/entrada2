package nl.sidn.iceberg.catalog.security;

import java.io.IOException;
import java.util.Collection;
import java.util.Enumeration;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.GenericFilterBean;


@Component
public class AuthenticationFilter extends GenericFilterBean {

  private static final String AUTH_TOKEN_HBEARER_PREFIX = "Bearer ";
  private static final String AUTH_TOKEN_HEADER_NAME = "Authorization";

  @Value("${catalog.security.token}")
  private String token;

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
      throws IOException, ServletException {

    printRequestHeaders((HttpServletRequest) request);
    String apiKey = ((HttpServletRequest) request).getHeader(AUTH_TOKEN_HEADER_NAME);

    if (StringUtils.equals(StringUtils.replace(apiKey, AUTH_TOKEN_HBEARER_PREFIX, ""), token)) {
      Authentication authentication =
          new ApiKeyAuthentication(apiKey, AuthorityUtils.NO_AUTHORITIES);
      SecurityContextHolder.getContext().setAuthentication(authentication);
      filterChain.doFilter(request, response);
    } else {
      HttpServletResponse httpResponse = (HttpServletResponse) response;
      httpResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    }
  }
  
  public void printRequestHeaders(HttpServletRequest req) {
    Enumeration names = req.getHeaderNames();
    if(names == null) {
      return;
    }
      while(names.hasMoreElements()) {
        String name = (String) names.nextElement();
         Enumeration values = req.getHeaders(name);
         if(values != null) {
           while(values.hasMoreElements()) {
               String value = (String) values.nextElement();
               System.out.println(name + " : " + value );
           }
         }
      }
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
