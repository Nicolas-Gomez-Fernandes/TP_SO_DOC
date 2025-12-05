#ifndef MAIN_H_
#define MAIN_H_

#include "inicializar_master.h"
#include "planificador.h"

//==============================
//== ARCHIVO DE CONFIGURACION ==
//==============================
char* puerto_escucha;
char* algoritmo_planificacion;
int tiempo_again;

t_log_level log_level;
t_log* master_logger;
t_config* master_config;

int fd_escucha;
int fd_cliente_query_control;
int fd_cliente_worker;

#endif
