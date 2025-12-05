#ifndef INICIALIZAR_WORKER_H_
#define INICIALIZAR_WORKER_H_

#include "gestor_worker.h"
#include "query_interpreter.h"
#include "memoria_interna.h" 


//===========================
//==== HEADERS FUNCIONES ====
//===========================
void inicializar_worker(char* archivo_config_path);
void inicializar_logs();
void inicializar_configs(char* archivo_config_path);
void conectar_a_master(char *ip, char *puerto, int *fd_destino);
void conectar_a_storage(char *ip, char *puerto, int *fd_destino);
void enviar_handshake_worker_master(int socket_fd, char *worker_id);
void enviar_handshake_worker_storage(int socket_fd, char *worker_id);
void recibir_handshake(int socket_fd, char *modulo);
void iniciar_conexiones_iniciales(char *worker_id);
void esperar_instrucciones_master();
void terminar_programa();
void inicializar_contexto(int query_id, int initial_pc, t_list* instrucciones_cargadas);

#endif