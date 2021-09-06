-- Создание отдельной схемы для контента:
CREATE SCHEMA IF NOT EXISTS content;

-- Жанры кинопроизведений:
CREATE TABLE IF NOT EXISTS content.genre (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR(80) NOT NULL,
  description VARCHAR(255),
  created_at timestamp with time zone,
  updated_at timestamp with time zone
);

-- Персоны (актеры, режиссеры, сценаристы)
CREATE TABLE IF NOT EXISTS content.person (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  full_name VARCHAR(255) NOT NULL,
  birth_date DATE,
  created_at timestamp with time zone,
  updated_at timestamp with time zone
);
CREATE INDEX person_full_name_idx ON content.person(full_name);

-- Фильмы
CREATE TYPE content.film_work_type AS ENUM ('movie', 'tv_show');
CREATE TABLE IF NOT EXISTS content.film_work (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
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
CREATE INDEX film_work_title_idx ON content.film_work(title);
CREATE INDEX film_work_creation_date_idx ON content.film_work(creation_date);

-- Жанры фильмов
CREATE TABLE IF NOT EXISTS content.genre_film_work (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  film_work_id uuid NOT NULL REFERENCES content.film_work(id) ON DELETE CASCADE ON UPDATE CASCADE,
  genre_id uuid NOT NULL REFERENCES content.genre(id) ON DELETE CASCADE ON UPDATE CASCADE,
  created_at timestamp with time zone
);
CREATE UNIQUE INDEX genre_film_work_idx ON content.genre_film_work (film_work_id, genre_id);

-- Персоны в фильмах
CREATE TYPE content.person_film_work_role AS ENUM ('actor', 'director', 'writer');
CREATE TABLE IF NOT EXISTS content.person_film_work (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  film_work_id uuid NOT NULL REFERENCES content.film_work(id) ON DELETE CASCADE ON UPDATE CASCADE,
  person_id uuid NOT NULL REFERENCES content.person(id) ON DELETE CASCADE ON UPDATE CASCADE,
  role content.person_film_work_role NOT NULL,
  created_at timestamp with time zone
);
CREATE UNIQUE INDEX person_film_work_idx ON content.person_film_work (film_work_id, person_id, role);
CREATE INDEX person_film_work_person_idx ON content.person_film_work(person_id);
