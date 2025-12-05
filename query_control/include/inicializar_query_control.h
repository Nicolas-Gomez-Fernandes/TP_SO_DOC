#ifndef INICIALIZAR_QUERY_CONTROL_H_
#define INICIALIZAR_QUERY_CONTROL_H_

#include "gestor_query_control.h"

//===========================
//==== HEADERS FUNCIONES ====
//===========================
void inicializarQueryControl();
void inicializar_logs();
void inicializar_configs();
void conectar_a_master(char* ip, char* puerto, int* fd_destino);
void enviar_handshake_query(int socket_fd, char* archivo_config, char* archivo_query, int prioridad);
void recibir_handshake(int socket_fd);
void iniciar_conexion_con_master(char* archivo_config, char* archivo_query, int prioridad);
void terminar_programa();

#endif