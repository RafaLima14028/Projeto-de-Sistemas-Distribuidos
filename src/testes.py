import threading
from time import sleep

import cliente
import controllerDatabase
import servidor


def print_conteudo() -> None:
    sleep(3)
    print("\n==========================================\nConteúdo de todos os servidores:")
    print("==========================================\nServidor 50051")
    cliente.get_range(50051, 'teste1', 'teste99', 0, 0)
    print("==========================================\nServidor 50052")
    cliente.get_range(50052, 'teste1', 'teste99', 0, 0)
    print("==========================================\nServidor 50053")
    cliente.get_range(50053, 'teste1', 'teste99', 0, 0)


bd_thread = threading.Thread(target=controllerDatabase.run, args=(True, True, True,))

server1_thread = threading.Thread(target=servidor.serve, args=(50051,))
server2_thread = threading.Thread(target=servidor.serve, args=(50052,))
server3_thread = threading.Thread(target=servidor.serve, args=(50053,))

bd_thread.start()
sleep(30)

server1_thread.start()
sleep(2)
server2_thread.start()
sleep(2)
server3_thread.start()
sleep(1)

print("Testes iniciados")

cliente.put(50051, 'teste1', 'valor1')
sleep(3)
cliente.put(50051, 'teste2', 'valor1')
sleep(3)
cliente.put(50051, 'teste3', 'valor1')

sleep(5)
cliente.put_all(50052, [('teste1', 'valor2'), ('teste4', 'valor1'), ('teste5', 'valor1'), ('teste2', 'valor2')])
sleep(3)
cliente.delete(50052, 'teste3')
sleep(3)
print_conteudo()

sleep(3)
cliente.trim(50053, 'teste1')

sleep(3)
cliente.delete_range(50051, 'teste1', 'teste4')
sleep(3)
print_conteudo()

sleep(3)
cliente.put(50052, 'teste,6', 'valor1')  # vírgula em 'teste,6' será removida pelo servidor
sleep(3)
cliente.put(50052, '', 'valor1')  # não deve inserir
sleep(3)
cliente.put(50052, 'teste0', '')  # não deve inserir
sleep(3)
print_conteudo()

sleep(3)
cliente.get(50051, 'teste-99', 0)  # não vai encontrar, chave inexistente

sleep(3)
cliente.delete_all(50053, ['teste5', 'teste6', 'teste7'])
sleep(3)
print_conteudo()

sleep(62)
print_conteudo()

print("Testes concluídos")

bd_thread.join()
server1_thread.join()
server2_thread.join()
server3_thread.join()
