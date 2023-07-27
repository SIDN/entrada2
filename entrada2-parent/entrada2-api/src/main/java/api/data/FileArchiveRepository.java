package api.data;

import org.springframework.data.jpa.repository.JpaRepository;
import api.data.model.FileArchive;
import api.data.model.FileIn;


public interface FileArchiveRepository extends JpaRepository<FileArchive, Long> {

  FileIn findByName(String name);

}