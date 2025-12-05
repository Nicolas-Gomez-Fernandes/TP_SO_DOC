#ifndef GESTOR_QUERY_CONTROL_H_
#define GESTOR_QUERY_CONTROL_H_

#include "../../utils/include/utils.h"

//==============================
//== ARCHIVO DE CONFIGURACION ==
//==============================
extern char* ip_master;
extern char* puerto_master;

extern t_log_level log_level;
extern t_log* query_control_logger;
extern t_config* query_control_config;
extern int fd_conexion_master;

#endif