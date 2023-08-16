create table file_in (
    id serial not null,
    name varchar(255) not null,
    server varchar(255) not null,
    location varchar(255) not null,
    created timestamp not null,
    served timestamp,  
  	size int not null,
    CONSTRAINT file_in_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_file_in_name ON file_in (name);
CREATE INDEX IF NOT EXISTS idx_file_in_created ON file_in (created);
CREATE INDEX IF NOT EXISTS idx_file_in_served ON file_in (served);

create table file_archive (
    id serial not null,
    name varchar(255) not null,
    server varchar(255) not null,
    location varchar(255) not null,
    created timestamp not null,
    served timestamp not null,
    size int not null,
    processed timestamp not null,
    rows int not null,
    time int not null,
    CONSTRAINT file_archive_pkey PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_file_archive_name ON file_archive (name);
CREATE INDEX IF NOT EXISTS idx_file_archive_created ON file_archive (created);
CREATE INDEX IF NOT EXISTS idx_file_archive_served ON file_archive (served);


create table auth_token (
    id serial not null,
    token varchar(32) not null,
    name varchar(255) not null,
    created timestamp not null,
    updated timestamp,
    enabled boolean not null,
    last_login timestamp,
    CONSTRAINT auth_token_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_auth_token_token ON auth_token (token);
