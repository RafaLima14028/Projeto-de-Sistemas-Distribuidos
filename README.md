<h1 align="center">Projeto de Sistemas Distribuídos</h1>

### Integrantes

* Rafael Alves de Lima - 12021BCC035 ([@RafaLima14028](https://github.com/RafaLima14028/))
* Mateus Rocha Resende - 11921BCC027 ([@matrocheetos](https://github.com/matrocheetos))

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

### Métodos do cache implementados (*temporário*)

- [x] insertAndUpdate
- [x] getByKeyVersion
- [x] delete

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

### Requisitos

- [x] Tabelas hash locais como cache para servidores
- [x] Documentação do esquema de dados das tabelas
- [x] Testes automatizados
- [x] Tratamento de erros
- [x] Execução de múltiplos clientes e servidores
- [ ] Replicação da base de dados
- [ ] Servidores são máquinas de estados determinística
- [ ] Três réplicas para o banco de dados

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
melhorando operações de escrira e leitura.

### Instalação e execução

Primeiramente, clone o projeto para sua máquina local:

```bash
git clone https://github.com/RafaLima14028/Projeto-de-Sistemas-Distribuidos.git
```

Instale as dependências necessárias e compile os arquivos gRPC:

```bash
cd Projeto-de-Sistemas-Distribuidos
chmod +x compile.sh
./compile.sh
```

### Link para o vídeo

[Parte 1 do Projeto](https://youtu.be/9ZDFBH2iPKQ)
