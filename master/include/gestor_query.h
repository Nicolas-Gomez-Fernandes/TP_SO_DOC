#ifndef GESTOR_QUERY_H
#define GESTOR_QUERY_H

#include "gestor_master.h"
#include "gestor_listas.h"
#include "gestor_aging.h"


int obtener_nuevo_id();
void inicializar_gestor_query();
query_t* crear_query(char* path, int prioridad, int fd_query_control);
void destruir_query(query_t* query);
void cambiar_estado_query(query_t* query, estado_query_t nuevo_estado);
void asignar_worker_a_query(query_t* query, int id_worker);
void destruir_gestor_query();
query_t* buscar_query_ready();
query_t* buscar_y_remover_query_por_id(t_lista* querys, int id);
int obtener_cantidad_querys();
query_t* remover_query_mayor_prioridad_ready();
query_t* get_query_mayor_prioridad_ready();
query_t* get_query_menor_prioridad_exec(int prioridad);

#endif // GESTOR_QUERY_H