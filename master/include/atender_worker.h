
#ifndef ATENDER_WORKER_H
#define ATENDER_WORKER_H

#include "gestor_query.h"
#include "gestor_worker.h"
#include "gestor_master.h"
#include "gestor_aging.h"
#include "planificador.h"


void* atender_worker(void* args);

#endif // ATENDER_WORKER_H