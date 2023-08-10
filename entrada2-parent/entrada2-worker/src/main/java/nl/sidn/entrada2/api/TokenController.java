package nl.sidn.entrada2.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import nl.sidn.entrada2.security.AuthenticationService;

@RestController()
@RequestMapping("/admin/token")
@ConditionalOnProperty(name = "entrada.mode", havingValue = "controller")
public class TokenController {
  
  @Autowired
  private AuthenticationService authService;

  @PostMapping("/{name}")
  public ResponseEntity<String> create(@PathVariable String name){
    
    return new ResponseEntity<>(authService.createToken(name), HttpStatus.OK);
    
  }
  
}
