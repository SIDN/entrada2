package nl.sidn.entrada2.worker.data;

import java.util.Optional;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import nl.sidn.entrada2.worker.data.model.AuthToken;

public interface AuthTokenRepository extends CrudRepository<AuthToken, Long> {
  
  Optional<AuthToken> findByToken(String token);

}