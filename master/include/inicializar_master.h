#ifndef INICIALIZAR_MASTER_H_
#define INICIALIZAR_MASTER_H_

#include "gestor_master.h"
#include "gestor_query.h"
#include "gestor_worker.h"
#include "atender_worker.h"
#include "atender_query.h"

//===========================
//==== HEADERS FUNCIONES ====
//===========================
void inicializar_master();
void inicializar_logs();
void inicializar_configs();
void iniciar_servidor_master();
void* atender_conexiones_master();
void terminar_programa();

#endif