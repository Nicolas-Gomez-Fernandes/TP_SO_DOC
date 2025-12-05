#include "../include/atender_query.h"

void* atender_query(void* args) {
    query_t* query = (query_t*)args;
    int fd_query_control = query->fd_query_control;
    int op;

    while (1) {
        // sleep(3); // para testear conexion de worker
        op = recibir_operacion(fd_query_control);

        switch (op) {

            case -1: {
                // Desconexión del Query Control
                if (query->estado == READY) {
                    // Remover de la cola READY y pasar a EXIT
                    buscar_y_remover_query_por_id(cola_ready, query->id);
                    if (query) {
                        cambiar_estado_query(query, EXIT);
                        buscar_y_remover_query_por_id(lista_querys,query->id);

                        // Log Obligatorio de Desconexión de Query Control
                        log_info(master_logger, "## Se desconecta un Query Control. Se finaliza la Query %d con prioridad %d. Nivel multiprocesamiento %d",
                            query->id, query->prioridad, obtener_cantidad_querys()); // Prioridad es un campo de la struct
                    } else {
                        log_warning(master_logger, "No se encontró la Query %d en READY al desconectarse el Query Control", query->id);
                    }
                    sem_post(&sem_replanificar); // Notificar al planificador
                }
                else if (query->estado == EXEC) {
                    // Notificar al worker para desalojar la query
                    worker_t* worker = buscar_worker_por_id(query->id_worker_asignado);
                    if (worker) {
                        // TODO: notificar al worker
                        // Log Obligatorio de Desalojo por Desconexión
                        log_info(master_logger, "## Se desaloja la Query %d del Worker %d", query->id, worker->id);
                        
                        enviar_operacion(worker->fd, QUERY_DOWN);
                        enviar_entero(worker->fd, master_logger, query->id);
                    } else {
                        log_warning(master_logger, "No se encontró el Worker asignado a la Query %d para desalojar tras desconexión de Query Control", query->id);
                    }
                    // Delego en el atender_worker la limpieza de la query y worker
                }
                return NULL;
                break;
            }
            default:
                log_warning(master_logger, "Operación <%d> desconocida recibida del Query Control %d", op, query->id);
                break;
        }
    }
    pthread_exit(NULL);
}