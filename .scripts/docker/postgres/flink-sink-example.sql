-- DDL

DROP table if EXISTS public.student;

CREATE TABLE public.student (
	id int4 NOT NULL,
	"name" varchar NULL,
	score int4 NULL,
	pass bool NULL DEFAULT false,
	CONSTRAINT student_pk PRIMARY KEY (id)
);

create or replace procedure public.upsert_student(
	_id bigint,
	_name varchar,
	_score int,
	_pass boolean
)
language plpgsql
as $$
begin
	insert into public.student values(_id, _name, _score, _pass) ON CONFLICT (id) 
	DO UPDATE SET name = _name, score = _score, pass = _pass;
end; $$;


-- DML
insert into public.student values('2', 'Song Li', '100', 'true') ON CONFLICT (id) DO UPDATE SET name = 'Song Li', score = '100', pass = 'true';
call public.upsert_student(2, 'Song Li', 90, false);
select * from public.student s;
