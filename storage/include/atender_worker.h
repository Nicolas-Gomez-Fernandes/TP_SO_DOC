#ifndef ATENDER_WORKER_STORAGE_H
#define ATENDER_WORKER_STORAGE_H

#include "gestor_storage.h"
#include "../include/funciones_storage.h"

typedef struct {
    int fd;
    char* worker_id;
} t_worker_info;

/**
 * @brief Hilo que atiende las solicitudes de un Worker conectado.
 * @param arg Puntero a una estructura t_worker_info asignada dinámicamente.
 * @return NULL al finalizar.
*/
void* atender_worker_storage(void* arg);

#endif 