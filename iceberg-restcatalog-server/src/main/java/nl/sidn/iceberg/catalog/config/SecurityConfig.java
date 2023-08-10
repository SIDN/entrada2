package nl.sidn.iceberg.catalog.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import nl.sidn.iceberg.catalog.security.AuthenticationFilter;

@Configuration
@EnableWebSecurity
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
            .antMatchers("/**").authenticated()
            .anyRequest().authenticated())
        .build();


    // return http
    // .csrf().disable()
    // .addFilterAfter(authenticationFilter, BasicAuthenticationFilter.class)
    // .authorizeHttpRequests(authorize -> authorize
    // .anyRequest().authenticated()
    // ).build();
    //
    // .addFilterAfter(authenticationFilter, BasicAuthenticationFilter.class)
    // .csrf(AbstractHttpConfigurer::disable)
    // .authorizeHttpRequests((authz) -> authz
    // .requestMatchers("/**").hasRole("USER")
    // .anyRequest().authenticated()
    // ).build();

  }

  // @Bean
  // RoleHierarchy roleHierarchy() {
  // RoleHierarchyImpl roleHierarchy = new RoleHierarchyImpl();
  // String hierarchy = "ROLE_ADMIN > ROLE_USER";
  // roleHierarchy.setHierarchy(hierarchy);
  // return roleHierarchy;
  // }

}
