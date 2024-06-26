package nl.sidn.entrada2.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.access.hierarchicalroles.RoleHierarchy;
import org.springframework.security.access.hierarchicalroles.RoleHierarchyImpl;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;

import nl.sidn.entrada2.security.AuthenticationFilter;

@Configuration
public class SecurityConfig {
  
  @Autowired
  private AuthenticationFilter authenticationFilter;

  @Bean
  SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
    
    return http
    .sessionManagement( (session) -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
    .addFilterAfter(authenticationFilter, BasicAuthenticationFilter.class)
    .csrf(AbstractHttpConfigurer::disable)
    .authorizeHttpRequests((authz) -> authz
    	.requestMatchers("/**").hasRole("ADMIN")
        .anyRequest().authenticated()
    ).build();

  }
  
  @Bean
  RoleHierarchy roleHierarchy() {
      return RoleHierarchyImpl.fromHierarchy( "ROLE_ADMIN > ROLE_USER");
  }

}