#ifndef MAIN_H_
#define MAIN_H_

#include "inicializar_query_control.h"
#include "atender_master.h"

//==============================
//== ARCHIVO DE CONFIGURACION ==
//==============================
char* ip_master;
char* puerto_master;

t_log_level log_level;
t_log* query_control_logger;
t_config* query_control_config;
int fd_conexion_master;

#endif