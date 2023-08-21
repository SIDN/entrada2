package nl.sidn.entrada2.security;

import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.GenericFilterBean;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Component
public class AuthenticationFilter extends GenericFilterBean {


//  @Autowired
//  private Environment env;
  
//  @Autowired
//  private ApplicationContext ctx;
  
  @Autowired
  private AuthenticationService authenticationService;

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
      throws IOException, ServletException {

    //if (Arrays.stream(env.getActiveProfiles()).anyMatch( p -> StringUtils.equalsIgnoreCase(p,"controller"))) {
      try {
       //Authentication authentication = ctx.getBean(AuthenticationService.class).getAuthentication((HttpServletRequest) request);
        Authentication authentication = authenticationService.getAuthentication((HttpServletRequest) request);
        SecurityContextHolder.getContext().setAuthentication(authentication);
      } catch (BadCredentialsException exp) {
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        httpResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      }
    //}

    filterChain.doFilter(request, response);
  }
}
