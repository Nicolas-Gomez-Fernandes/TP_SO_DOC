#ifndef INICIALIZAR_WORKER_H_
#define INICIALIZAR_WORKER_H_

#include "gestor_storage.h"
#include "atender_worker.h"
#include "../include/funciones_storage.h"

//===========================
//==== HEADERS FUNCIONES ====
//===========================
void inicializar_storage(char* path);
void inicializar_logs();
void inicializar_configs(char* path);
void iniciar_servidor_storage();
void inicializar_superbloque();
void atender_conexiones_storage();
void terminar_programa();

#endif