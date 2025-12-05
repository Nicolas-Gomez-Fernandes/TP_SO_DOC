#ifndef ATENDER_MASTER_H
#define ATENDER_MASTER_H

#include "gestor_query_control.h"

/* Función del hilo que procesa mensajes provenientes del Master */
void* atender_master(void* args);

#endif /* ATENDER_MASTER_H */