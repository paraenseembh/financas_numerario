-- Inicialização dos schemas da arquitetura medalhão
-- Execute como superusuário ou com permissões adequadas

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS prata;
CREATE SCHEMA IF NOT EXISTS ouro;

-- Permissões para o usuário da aplicação (ajuste conforme necessário)
GRANT USAGE ON SCHEMA bronze TO numerario_user;
GRANT USAGE ON SCHEMA prata  TO numerario_user;
GRANT USAGE ON SCHEMA ouro   TO numerario_user;

GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA bronze TO numerario_user;
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA prata  TO numerario_user;
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA ouro   TO numerario_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze TO numerario_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA prata  TO numerario_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ouro   TO numerario_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES    TO numerario_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA prata  GRANT ALL ON TABLES    TO numerario_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA ouro   GRANT ALL ON TABLES    TO numerario_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON SEQUENCES TO numerario_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA prata  GRANT ALL ON SEQUENCES TO numerario_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA ouro   GRANT ALL ON SEQUENCES TO numerario_user;
