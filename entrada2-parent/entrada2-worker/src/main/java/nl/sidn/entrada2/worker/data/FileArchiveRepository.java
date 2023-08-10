package nl.sidn.entrada2.worker.data;

import org.springframework.data.jpa.repository.JpaRepository;
import nl.sidn.entrada2.worker.data.model.FileArchive;
import nl.sidn.entrada2.worker.data.model.FileIn;

public interface FileArchiveRepository extends JpaRepository<FileArchive, Long> {

  FileIn findByName(String name);

}