import threading

import cliente
import servidor

server1_thread = threading.Thread(target=servidor.serve, args=(50051,))
server2_thread = threading.Thread(target=servidor.serve, args=(50052,))
server3_thread = threading.Thread(target=servidor.serve, args=(50053,))

server1_thread.start()
server2_thread.start()
server3_thread.start()

cliente.put(50051, 'teste1', 'valor1')
cliente.put(50051, 'teste2', 'valor1')
cliente.put(50051, 'teste3', 'valor1')
# cliente.get_all(50053, [('teste1', 0), ('teste4', 0), ('teste5', 0)])

cliente.put_all(50052, [('teste1', 'valor2'), ('teste4', 'valor1'), ('teste5', 'valor1'), ('teste2', 'valor2')])
# cliente.get_all(50051, [('teste1', 0), ('teste4', 0), ('teste5', 0), ('teste2', 0)])
# cliente.get(50053, 'teste2', 0)

cliente.delete(50052, 'teste3')
# cliente.get_range(50053, 'teste1', 'teste4', 0, 0)

cliente.trim(50052, 'teste1')
# cliente.get(50051, 'teste1', 0)

cliente.delete_range(50051, 'teste1', 'teste4')
# cliente.get_range(50053, 'teste1', 'teste4', 0, 0)

cliente.put(50052, 'teste6', 'valor1')
cliente.delete_all(50053, ['teste5', 'teste6', 'teste7'])
# cliente.get_range(50053, 'teste5', 'teste7', 0, 0)

print("\nFinal - conteúdo de todos os servidores:\n")
print("==========================================\nServidor 50051")
cliente.get_range(50051, 'teste1', 'teste7', 0, 0)
print("==========================================\nServidor 50052")
cliente.get_range(50052, 'teste1', 'teste7', 0, 0)
print("==========================================\nServidor 50053")
cliente.get_range(50053, 'teste1', 'teste7', 0, 0)

print("Testes concluídos")

server1_thread.join()
server1_thread.join()
server1_thread.join()
