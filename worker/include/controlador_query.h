// controlador_query.h
#ifndef CONTROLADOR_QUERY_H
#define CONTROLADOR_QUERY_H

#include "gestor_worker.h"
#include "query_interpreter.h"
#include "inicializar_worker.h"

//===========================
//==== HEADERS FUNCIONES ====
//===========================
void manejar_interrupcion_query(int fd_master);

#endif
