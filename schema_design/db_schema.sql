-- Создание отдельной схемы для контента:
CREATE SCHEMA IF NOT EXISTS content;

-- Жанры кинопроизведений:
CREATE TABLE IF NOT EXISTS content.genre (
  id uuid PRIMARY KEY,
  name VARCHAR(80) NOT NULL,
  description VARCHAR(255),
  created_at timestamp with time zone,
  updated_at timestamp with time zone
);

-- Персоны (актеры, режиссеры, сценаристы)
CREATE TABLE IF NOT EXISTS content.person (
  id uuid PRIMARY KEY,
  full_name VARCHAR(255) NOT NULL,
  birth_date DATE,
  created_at timestamp with time zone,
  updated_at timestamp with time zone
);

-- Фильмы
CREATE TYPE content.film_work_type AS ENUM ('movie');
CREATE TABLE IF NOT EXISTS content.film_work (
  id uuid PRIMARY KEY,
  title VARCHAR(255) NOT NULL,
  description TEXT,
  creation_date DATE,
  certificate VARCHAR(255),
  file_path VARCHAR(255),
  rating FLOAT,
  type content.film_work_type NOT NULL,
  created_at timestamp with time zone,
  updated_at timestamp with time zone
);

-- Жанры фильмов
CREATE TABLE IF NOT EXISTS content.genre_film_work (
  id uuid PRIMARY KEY,
  film_work_id uuid NOT NULL,
  genre_id uuid NOT NULL,
  created_at timestamp with time zone,

  FOREIGN KEY (film_work_id) REFERENCES content.film_work(id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  FOREIGN KEY (genre_id) REFERENCES content.genre(id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);
CREATE UNIQUE INDEX genre_film_work_idx ON content.genre_film_work (film_work_id, genre_id);

-- Персоны в фильмах
CREATE TYPE content.person_film_work_role AS ENUM ('actor', 'director', 'writer');
CREATE TABLE IF NOT EXISTS content.person_film_work (
  id uuid PRIMARY KEY,
  film_work_id uuid NOT NULL,
  person_id uuid NOT NULL,
  role content.person_film_work_role NOT NULL,
  created_at timestamp with time zone,

  FOREIGN KEY (film_work_id) REFERENCES content.film_work(id)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  FOREIGN KEY (person_id) REFERENCES content.person(id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);
CREATE UNIQUE INDEX person_film_work_idx ON content.person_film_work (film_work_id, person_id, role);