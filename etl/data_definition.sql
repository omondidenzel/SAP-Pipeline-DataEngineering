create table 'your_schema_name'.'your_table_name'(
		company_id varchar(10),
		creation_date timestamp,
		posting_date timestamp,
		journal_id int not null,
		sourcedoc_id int not null,
		account_id int not null,
		account_name varchar(100),
		debit float,
		credit float		
);