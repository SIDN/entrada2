package nl.sidn.entrada2.worker.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.access.hierarchicalroles.RoleHierarchy;
import org.springframework.security.access.hierarchicalroles.RoleHierarchyImpl;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.expression.DefaultWebSecurityExpressionHandler;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import nl.sidn.entrada2.worker.security.AuthenticationFilter;

@Configuration
public class SecurityConfig {
  
  @Autowired
  private AuthenticationFilter authenticationFilter;

  @Bean
  SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
      
    
    return http
    .addFilterAfter(authenticationFilter, BasicAuthenticationFilter.class)
    .csrf(AbstractHttpConfigurer::disable)
    .authorizeHttpRequests((authz) -> authz
        .requestMatchers("/api/v1/admin/**").hasRole("ADMIN")
        .requestMatchers("/api/v1/**").hasRole("USER")
        .anyRequest().authenticated()
    ).build();

    //
    //
    // return http
    // .addFilterAfter(authenticationFilter, BasicAuthenticationFilter.class)
    // .csrf(AbstractHttpConfigurer::disable)
    // .
    // .hasRole("STAFF")
    // .build();

  }
  
  @Bean
  RoleHierarchy roleHierarchy() {
      RoleHierarchyImpl roleHierarchy = new RoleHierarchyImpl();
      String hierarchy = "ROLE_ADMIN > ROLE_USER";
      roleHierarchy.setHierarchy(hierarchy);
      return roleHierarchy;
  }
  
//  @Bean
//  DefaultWebSecurityExpressionHandler webSecurityExpressionHandler() {
//      DefaultWebSecurityExpressionHandler expressionHandler = new DefaultWebSecurityExpressionHandler();
//      expressionHandler.setRoleHierarchy(roleHierarchy());
//      return expressionHandler;
//  }

}