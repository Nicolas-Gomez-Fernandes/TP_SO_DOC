#ifndef GESTOR_WORKER_H
#define GESTOR_WORKER_H

#include "gestor_master.h"
#include "gestor_listas.h"

// Funciones de gestión de workers
void inicializar_gestor_worker();
void destruir_gestor_worker();

worker_t* crear_worker(int id, int fd);
void destruir_worker(worker_t* w);

void agregar_worker(worker_t* worker);
worker_t* buscar_worker_por_id(int id);
worker_t* buscar_worker_libre();

void cambiar_estado_worker(worker_t* worker, estado_worker_t nuevo_estado);
void asignar_query_a_worker(worker_t* worker, int id_query);
void liberar_worker(worker_t* worker);

void notificar_asignacion_a_worker(worker_t* worker, query_t* query);

int obtener_cantidad_workers();
void eliminar_worker_por_id(int id);

void enviar_senal_desalojo(int id_worker, int id_query) ;

#endif