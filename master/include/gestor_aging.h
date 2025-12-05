#ifndef GESTOR_AGING_H
#define GESTOR_AGING_H

#include "gestor_master.h"

// Funciones públicas
void crear_hilo_aging_para_query(query_t* query);
void detener_aging_para_query(query_t* query);

#endif // GESTOR_AGING_H