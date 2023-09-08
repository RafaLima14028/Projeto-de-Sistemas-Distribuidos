<h1 align="center">Projeto de Sistemas Distribuídos</h1>

<h3>Integrantes</h3>

* Rafael Alves de Lima - 12021BCC035
* Mateus Rocha Resende - 11921BCC027

<h3>Métodos implementados</h3>

- [x] Put
- [x] Get
- [x] Del
- [x] Trim
- [x] GetRange
- [x] DelRange
- [x] PutAll 
- [x] GetAll
- [x] DelAll

<h3>Esquema de dados</h3>
Para o armazenamento dos dados foi utilizado um dicionário, 
de forma que cada chave (string) tem uma lista de tuplas contendo 
versão (inteiro) e um valor (string). Por exemplo:

```
dictionary['key1'] = [(version1, value1), (version2, value2), ...]
dictionary['key2'] = [(version3, value3)] 
```

<h3>Requisitos</h3>

- [x] Tabelas hash locais como cache para servidores
- [x] Documentação do esquema de dados das tabelas
- [ ] Testes automatizados
- [x] Tratamento de erros
- [ ] Execução de múltiplos clientes e servidores
- [ ] Propagação _pub-sub_

<h3>Como instalar</h3>
Primeiramente, clone o projeto para sua máquina local.<p/>
```
git clone https://github.com/RafaLima14028/Projeto-de-Sistemas-Distribuidos.git
```

Instale as dependências necessárias.<p/>
```
pip install grpcio grpcio-tools
```

<h3>Link para o vídeo</h3>
TODO: Colocar o link
