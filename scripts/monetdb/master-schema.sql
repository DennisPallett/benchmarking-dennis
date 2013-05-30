CREATE TABLE instance (
	instance_host VARCHAR(100) NOT NULL,
	instance_port SMALLINT NOT NULL
);

CREATE TABLE tenant (
	tenant_id INT PRIMARY KEY NOT NULL,
	tenant_host VARCHAR(100) NOT NULL,
	tenant_port SMALLINT NOT NULL
);