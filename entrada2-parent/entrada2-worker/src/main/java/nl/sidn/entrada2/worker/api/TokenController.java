package nl.sidn.entrada2.worker.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import nl.sidn.entrada2.worker.security.AuthenticationService;

@RestController()
@RequestMapping("/admin/token")
public class TokenController {
  
  @Autowired
  private AuthenticationService authService;

  @PostMapping("/{name}")
  public ResponseEntity<String> create(@PathVariable String name){
    
    return new ResponseEntity<>(authService.createToken(name), HttpStatus.OK);
    
  }
  
}
