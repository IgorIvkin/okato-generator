# Генератор записей по данным ОКАТО

Этот генератор использует свободно распространяемую базу ОКАТО - общероссийского
классификатора административно-территориальных образований - и сохраняет данные по ней
в таблицу БД с соблюдением иерархии.

Базу данных ОКАТО можно бесплатно скачать на сайте Росстата - https://rosstat.gov.ru/opendata/7708234640-okato

Проект был написан для личного использования, поэтому имеет ограниченную настраиваемость.

В качестве СУБД предполагается **PostgreSQL** с таблицей следующей структуры:

```sql
CREATE TABLE public.places
(
    id                       bigserial              NOT NULL,
    title                    character varying(255) NOT NULL,
    title_with_pronunciation character varying(255) NOT NULL,
    country_id               character varying(2)   NOT NULL,
    parent_place_id          bigint                 NULL,
    okato_code               character varying(255) NULL,
    CONSTRAINT place_pkey PRIMARY KEY (id)
);
```

Поле `title_with_pronunciation` используется для того, чтобы хранить значение с
предлогом, например "в Москве".

## Параметры запуска

```
java -jar okato-generator.jar <PATH_TO_INPUT.CSV> <JDBC_URL_TO_DATABASE>
<DATABASE_USER> <DATABASE_PASSWORD>
```

Например:

```
java -jar okato-generator.jar /home/okato/okato.csv jdbc:postgresql://localhost:5432/database_name user pwd
```