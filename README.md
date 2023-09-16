<h1 align="center">Projeto de Sistemas Distribuídos</h1>

### Integrantes

* Rafael Alves de Lima - 12021BCC035
* Mateus Rocha Resende - 11921BCC027

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

### Requisitos

- [x] Tabelas hash locais como cache para servidores
- [x] Documentação do esquema de dados das tabelas
- [x] Testes automatizados
- [x] Tratamento de erros
- [x] Execução de múltiplos clientes e servidores
- [x] Propagação _pub-sub_

### Esquema de dados

Para o armazenamento dos dados foi utilizado um dicionário,
de forma que cada chave (string) tem uma lista de tuplas contendo
versão (inteiro) e um valor (string). Por exemplo:

```python
dictionary['key1'] = [(version1, value1), (version2, value2), ...]
dictionary['key2'] = [(version3, value3)] 
```

As mensagens _pub-sub_ foram formatadas da seguinte forma:

```python
# inserção:
topic='projeto-sd/insert'
payload='key,version,value'

# remoção:
topic='projeto-sd/delete'
payload='key'

# trim:
topic='projeto-sd/trim'
payload='key'
```

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

Inicialize o _mosquitto_, servidor e cliente:

```bash
mosquitto -v
./server.sh
./client.sh
```

### Link para o vídeo

TODO: Colocar o link
