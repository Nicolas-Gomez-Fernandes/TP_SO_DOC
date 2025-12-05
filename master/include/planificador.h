#ifndef PLANIFICADOR_H
#define PLANIFICADOR_H

#include "gestor_master.h"
#include "gestor_query.h"
#include "gestor_worker.h"
#include "gestor_aging.h"

void iniciar_planificador();
void detener_planificador();
void* ciclo_planificador(void* arg);

datos_ejecucion_t* crear_datos_para_ejecucion(query_t* query, worker_t* worker);
#endif // PLANIFICADOR_H