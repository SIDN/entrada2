package api.data;

import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.PagingAndSortingRepository;
import api.data.model.FileIn;

public interface FileInRepository extends PagingAndSortingRepository<FileIn, Long>, CrudRepository<FileIn, Long> {

    Optional<FileIn> findByName(String name);
    
    List<FileIn> findByServedIsNull(Pageable pageable);
    
        

}
