package nl.sidn.entrada2.data;

import org.springframework.data.jpa.repository.JpaRepository;
import nl.sidn.entrada2.data.model.FileArchive;
import nl.sidn.entrada2.data.model.FileIn;

public interface FileArchiveRepository extends JpaRepository<FileArchive, Long> {

  FileIn findByName(String name);

}