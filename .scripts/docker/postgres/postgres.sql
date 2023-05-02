DROP table IF EXISTS public.student;

CREATE TABLE public.student (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	"name" varchar NOT NULL,
	updated timestamp NOT NULL,
	CONSTRAINT student_pk PRIMARY KEY (id)
);

-- alter table public.student replica identity full;

insert into public.student (name, updated) values ('Donald Trump', now());
insert into public.student (name, updated) values ('Song Li', now());

select id, name, updated from public.student;

-- Create a user 
create USER song WITH ENCRYPTED PASSWORD 'pwd-1';
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO song;

-- Alter the password
alter USER song WITH ENCRYPTED PASSWORD 'pwd-2';