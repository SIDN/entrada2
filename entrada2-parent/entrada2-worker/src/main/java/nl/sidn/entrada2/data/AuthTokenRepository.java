package nl.sidn.entrada2.data;

import java.util.Optional;
import org.springframework.data.repository.CrudRepository;
import nl.sidn.entrada2.data.model.AuthToken;

public interface AuthTokenRepository extends CrudRepository<AuthToken, Long> {
  
  Optional<AuthToken> findByToken(String token);

}