- `docker-compose up -d`: Inicia os serviços e cria as networks/volumes definidos no arquivo `docker-compose.yml`
- `docker-compose down -v`: Pausa os containeres em execução no docker e remove os volumes criados
- `docker exec -it airflow cat standalone_admin_password.txt`: Capturar a senha do airflow


## Para acessar a máquina do Airflow:

- `docker ps`: Lista os containers ativos
- `docker exec -it <container name> /bin/bash`: Executa o bash na máquina do airflow
- `ls -la <folder>`: Lista os arquivos dentro da pasta

## TO DO
- O que é dockerfile?