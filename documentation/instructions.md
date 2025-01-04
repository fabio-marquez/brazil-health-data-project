## Passos:

### 1. Minio: Repositório de dados aberto

- Criar um arquivo docker-compose.yml com a definição da imagem do minio a ser usada
- `docker-compose up -d` para iniciar o serviço do minio e executar os comandos do entrypoint para criar as pastas landing/bronze/silver/gold

### 2. Airflow: Orquestrador

- Adicionar as definições do airflow no docker compose (postgres + airflow)
- Criar um arquivo `requirements.txt` com as bibliotecas e ferramentas que serão utilizadas
- Criar uma pasta `config_airflow` e dentro dela um arquivo `airflow.Dockerfile` com as ações a serem executadas quando o airflow for subir
- Criar uma pasta `airflow/dags` para mapear dags no computador local para o airflow
- Criar as dags na pasta acima para depois executar no airflow
