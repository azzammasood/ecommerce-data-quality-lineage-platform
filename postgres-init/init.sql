CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
\c airflow
GRANT ALL ON SCHEMA public TO airflow;

CREATE USER marquez WITH PASSWORD 'marquez';
CREATE DATABASE marquez;
GRANT ALL PRIVILEGES ON DATABASE marquez TO marquez;
\c marquez
GRANT ALL ON SCHEMA public TO marquez;

CREATE USER ecommerce_user WITH PASSWORD 'ecommerce_pass';
CREATE DATABASE ecommerce;
GRANT ALL PRIVILEGES ON DATABASE ecommerce TO ecommerce_user;
\c ecommerce
GRANT ALL ON SCHEMA public TO ecommerce_user;
