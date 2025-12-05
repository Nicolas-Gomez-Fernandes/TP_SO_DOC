#include "../include/planificador.h"

static pthread_t hilo_planificador;
static bool planificador_activo = true;

void* ciclo_planificador(void* arg) {
    while (planificador_activo) {
        log_info(master_logger, "Esperando evento de replanificación...");
        sem_wait(&sem_replanificar);

        log_info(master_logger, "Replanificando...");
        // 1. Verificar si hay queries en READY
        if (esta_vacia(cola_ready)) {
            log_info(master_logger, "No hay queries en estado READY");
            continue;
        }

        // 2. Buscar workers libres
        worker_t* worker = buscar_worker_libre();
        if (!worker) {
            log_info(master_logger, "No hay workers libres");

            if (strcmp(algoritmo_planificacion, "PRIORIDADES") == 0) {
                log_info(master_logger, "Evaluando posible desalojo...");

                query_t* ref = get_query_mayor_prioridad_ready();
                query_t* a_desalojar = get_query_menor_prioridad_exec(ref->prioridad);

                if (!a_desalojar) 
                    log_info(master_logger, "No hay queries de menor prioridad en ejecución para desalojar");
                else 
                    enviar_senal_desalojo(a_desalojar->id_worker_asignado, a_desalojar->id);
            }
            continue; // si no hay worker siempre implica replanificar
        }

        // 3. Buscar la query READY
        query_t* query = buscar_query_ready();

        // detener aging para la query seleccionada
        detener_aging_para_query(query);

        // 4. Asinacciones
        asignar_worker_a_query(query, worker->id);
        asignar_query_a_worker(worker, query->id);

        // 5. Notificar al worker
        notificar_asignacion_a_worker(worker, query);

    
    }
    return NULL;
}

void iniciar_planificador() {
    planificador_activo = true;
    pthread_create(&hilo_planificador, NULL, ciclo_planificador, NULL);
    pthread_join(hilo_planificador, NULL);
}

void detener_planificador() {
    planificador_activo = false;
    sem_post(&sem_replanificar);
    pthread_join(hilo_planificador, NULL);
}

datos_ejecucion_t* crear_datos_para_ejecucion(query_t* query, worker_t* worker) {
    datos_ejecucion_t* datos = malloc(sizeof(datos_ejecucion_t));
    datos->query = query;
    datos->worker = worker;
    return datos;
}