package nl.sidn.entrada2.worker.security;

import java.io.IOException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
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
@Order(1)
public class AuthenticationFilter extends GenericFilterBean {
  
  @Autowired
  private AuthenticationService authService;

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
    throws IOException, ServletException {
      try {
          Authentication authentication = authService.getAuthentication((HttpServletRequest) request);
          SecurityContextHolder.getContext().setAuthentication(authentication);
      } catch (BadCredentialsException exp) {
          HttpServletResponse httpResponse = (HttpServletResponse) response;
          httpResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      }

      filterChain.doFilter(request, response);
  }
}