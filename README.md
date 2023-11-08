<h1 align="center">Projeto de Sistemas Distribuídos</h1>

### Integrantes

* Rafael Alves de Lima - 12021BCC035 ([@RafaLima14028](https://github.com/RafaLima14028/))
* Mateus Rocha Resende - 11921BCC027 ([@matrocheetos](https://github.com/matrocheetos))

---

### Métodos implementados

- [x] Put
- [x] Get
- [x] Del
- [x] Trim
- [x] GetRange
- [x] DelRange
- [x] PutAll
- [x] GetAll
- [x] DelAll

### A fazer ainda (*temporário*)

- [x] Permitir o cache trabalhar com outras replicas além da 1 (db1), a porta é a 40400 que é referente ao
socket da replica 1. Isso está no handlesJSON.
- [x] Remover a exceção gerada pelo handlesJSON, quando uma conexão é fechada inesperadamente no recv.
- [x] Métodos do handlesJSON tentam refazer a conexão caso a réplica em uso falhe.
- [x] Mudar a forma como o tempo é guardado, dividindo por 10.000 para guardar e multiplica por 10.000
para retornar ao cliente.
- [x] Tratar fechamento inesperado com o recv do cache.
- [x] Verificar a replicação entre os bancos de dados.
- [x] Ajustar a reconexão do servidor com as réplicas.
- [x] No servidor há algumas funções sem cache.
- [ ] Problemas com Threads (processos) do SyncObj, conversando pela Thread (gera erro).
- [ ] Quando ensera o servidor, o controller espera conectar com o próximo db, deve ficar
verificando qual vai ser o próximo a ser conectado (controllerDatabase.py linha 222 - no while True)
- [x] Ajustar funções do cache:
  - [x] DelRange (está ficando travada).
  - [x] DelAll (está ficando travada).
- [x] replica.sh receber quais dbs devem ser levantados.
- [ ] Implementar os novos testes.
- [x] Atualizar README.md com as instruções de compilação, inicialização e uso do controllerDatabase, servidor e cliente.
- [x] Definir argumentos para replica.sh / controllerDatabase.py.

### Métodos do cache implementados (*temporário*)

- [x] insertAndUpdate
- [x] getByKeyVersion
- [x] getRangeByKeyVersion
- [x] trim
- [x] delete
- [x] delRange

### Regras para atualizar o cache (*temporário*)

- **Put**: Verificar se o cache está dentro de um tempo válido, senão atualiza do db.
  Se o dado já existe, retorna do cache.
- **Get**: Verifica se existe ou não no cache os dados.
  Se os dados existem, verifica se está no tempo válido e retorna, senão atualiza o
  cache e retorna.
  Se os dados não existirem, verifica se está no tempo válido e retorna, senão
  atualiza o cache e retorna.
- **Del**: Busca a chave no cache usando a função Get e
  executa a operação de excluir todas as chaves do cache e depois executa no
  banco de dados também.
- **Trim**: Busca a chave no cache usando a função Get e
  executa a operação de excluir todas as chaves do cache,
  guarda os últimos dados que haviam e depois executa no
  banco de dados também.
- **GetRange**: Verifica se existe ou não no cache os dados. Se os dados existirem, verifica
  se está no tempo válido e retorna, senão atualiza o cache e retorna.
  Se os dados não existirem, verifica se está no tempo válido e retorna, senão atualiza o cache e retorna.
- **DelRange**: Busca a chave no cache usando a função Get e
  executa a operação de excluir todas as chaves do cache e depois executa no
  banco de dados também.

### Requisitos

- [x] Tabelas hash locais como cache para servidores
- [x] Documentação do esquema de dados das tabelas
- [x] Testes automatizados
- [x] Tratamento de erros
- [x] Execução de múltiplos clientes e servidores
- [x] Três réplicas para o banco de dados
- [x] Servidores são máquinas de estados determinística

---

### Esquema de dados

#### Cache:

Para o armazenamento do cache foi utilizado um dicionário,
de forma que cada chave (string) tem uma lista de tuplas contendo
versão (inteiro), um valor (string) e o momento na qual a versão e
o valor atual foram inseridos no cache. Esse último valor permite
saber à quanto tempo esse valor e versão já estão inseridos, isso indica
a "validade desses dados". O cache tem um limite de 1 minuto (60 segundos),
após isso existe a necessidade de um novo acesso ao banco de dados e
atualizar o cache para manter a consistência. Exemplo de como os dados
são alocados no dicionário:

```python
dictionary['key1'] = [((version1, value1), time_inseret_into_cache1), ((version2, value2), time_inseret_into_cache2), ...]
dictionary['key2'] = [((version3, value3), time_inseret_into_cache3)]
```

#### Banco de dados:

Para o armazenamento dos dados de maneira persistente foi utilizando o LMDB.
A ideia empregada é muito similar a do cache, para cada chave (string) existe
uma tupla contendo uma versão (inteiro) e um valor (string). No momento de abertura
do arquivo do banco de dados são passados dois parâmetros, um deles é o
sync=True indicando que operações de gravação sejam sincronizadas com o disco
imediatamente. O outro parâmetro é o writemap=True, isso permite que o banco
de dados seja mapeamento para a memória principal do sistema operacional,
melhorando operações de escrira e leitura. O decorador @replicated_sync, garante que as operações marcadas com ele
ocorram de maneira síncrona para todos os nós do sistema.

#### Comunicação entre o cache e banco de dados:

Para a comunicação entre o banco de dados e o servidor, o cache faz a intermediação entre eles.
Sempre que o dados expirou no cache, é recorrido ao banco de dados que atualiza o cache e
para o retorno dos dados ao servidor. Essa intermediação entre banco de dados e cache ocorre
por meio de um JSON de envio e outro de resposta para o cache, esse envio e resposta só é possível por conta
do socket. O JSON envio com o cache vai com a função de requisição e os dados que são necessários para a requisição
no banco de dados, sendo enviado via a comunicação entre ambos os lados. Já o lado do banco de dados ao receber
o JSON do cache, é verificado qual função que requisitou para saber qual será a resposta, ou seja, é atualiza o
banco de dados e as informações são retornadas ao cache por meio de um JSON que é enviado por um socket. Caso a réplica
em uso do cache falhe, tenta-se conectar as outras, mas se não houver nenhum é gerada uma exceção.

---

### Instalação e execução

Primeiramente, clone o projeto para sua máquina local e abra a pasta:

```bash
git clone https://github.com/RafaLima14028/Projeto-de-Sistemas-Distribuidos.git
cd Projeto-de-Sistemas-Distribuidos
```

Crie o ambiente virtual, instale as dependências necessárias e compile os arquivos gRPC:

```bash
chmod +x compile.sh
./compile.sh
```

Inicialize as réplicas, devem ser usados os parâmetros bd1, bd2 e/ou bd3:

```bash
./replica.sh -bd1 -bd2 -bd3
```

Inicialize o(s) servidor(es) especificando uma porta:

```bash
./server.sh 50051
./server.sh 50052
...
```

Inicialize o(s) cliente(s) com a porta correspondente ao servidor que se deseja conectar:

```bash
./client.sh 50051
./client.sh 50052
...
```

---

### Link para os vídeos

[Parte 1 do Projeto](https://youtu.be/9ZDFBH2iPKQ)

[Parte 2 do Projeto]() **(Em breve)**
