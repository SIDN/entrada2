package nl.sidn.entrada2.data.model;

import java.time.LocalDateTime;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
@Table(name = "file_archive")
public class FileArchive {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;
  private String name;
  private String server;
  private String location;
  private LocalDateTime created;
  private LocalDateTime served;
  private Integer size;
  private LocalDateTime processed;
  private Integer rows;
  private Integer time;


  public static FileArchive.FileArchiveBuilder fromFileIn(FileIn fin) {
    return FileArchive.builder()
        .name(fin.getName())
        .server(fin.getServer())
        .location(fin.getLocation())
        .created(fin.getCreated())
        .served(fin.getServed())
        .size(fin.getSize());
  }
}
