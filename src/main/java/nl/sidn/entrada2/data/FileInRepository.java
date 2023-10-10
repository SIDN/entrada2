package nl.sidn.entrada2.data;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;
import nl.sidn.entrada2.data.model.FileIn;

public interface FileInRepository
    extends PagingAndSortingRepository<FileIn, Long>, CrudRepository<FileIn, Long> {

  Optional<FileIn> findByName(String name);

  List<FileIn> findByServedIsNull(Pageable pageable);

  @Modifying
  @Query(value = "UPDATE file_in SET served = null WHERE served IS NOT NULL AND served < :max_date",
      nativeQuery = true)
  int resetExpired(@Param("max_date") LocalDateTime max);

}
