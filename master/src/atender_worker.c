#include "../include/atender_worker.h"

void* atender_worker(void* args) {
    worker_t* worker = (worker_t*)args;
    int fd_worker = worker->fd;
    int id_query;
    int op;
    int pc;

    while (1) {
        // sleep(5); // para test ;
        // op = QUERY_DOWN;
        // (void) fd_worker;
        // op = END; // Simulación para este ejemplo
        op = recibir_operacion(fd_worker);


        switch (op) {
            case QUERY_FINALIZADA:
            case QUERY_ERROR: {
                id_query = recibir_entero(fd_worker, master_logger);
                query_t* query = buscar_y_remover_query_por_id(cola_exec, id_query);
                char* motivo_error;

                if (query) {
                    if (op == QUERY_FINALIZADA) {
                        log_info(master_logger, "## Se terminó la Query %d en el Worker %d (Finalización correcta)", query->id, worker->id);
                        motivo_error = "(Finalización correcta)";
                    } else {
                        // Si es un error, recibimos el string con el motivo
                        motivo_error = recibir_string(fd_worker, master_logger);
                        if (motivo_error) {
                            log_info(master_logger, "## Se terminó la Query %d en el Worker %d con error", query->id, worker->id);
                        } else {
                            log_error(master_logger, "## Se terminó la Query %d en el Worker %d con error (No se pudo recibir el motivo)", query->id, worker->id);
                        }
                    }
                    enviar_operacion(query->fd_query_control, END);
                    enviar_string(query->fd_query_control, master_logger, motivo_error);
                    // free(motivo_error);
                    cambiar_estado_query(query, EXIT);
                }

                liberar_worker(worker);
                sem_post(&sem_replanificar);
                break;
            }
            case QUERY_DOWN: {
                // TODO: que lo reciba por socket
                id_query = recibir_entero(fd_worker, master_logger);
                // id_query = worker->query_asignada; 

                // Remover la query de la cola de ejecución
                query_t* query = buscar_y_remover_query_por_id(cola_exec, id_query);

                if (query) {
                    cambiar_estado_query(query, EXIT);

                    // Remover la query de la lista global para actualizar el grado de multiprocesamiento
                    buscar_y_remover_query_por_id(lista_querys, id_query);

                    // Log Obligatorio de Desconexión de Query Control
                    log_info(master_logger, "## Se desconecta un Query Control. Se finaliza la Query %d con prioridad %d. Nivel multiprocesamiento %d",
                        query->id, query->prioridad, obtener_cantidad_querys());

                    destruir_query(query);
                } else {
                    log_error(master_logger, "No se encontró la Query %d en EXEC para liberar tras QUERY_DOWN", id_query);
                }

                liberar_worker(worker);
                sem_post(&sem_replanificar);
                break;
            }
            case QUERY_READ_OK: {
                // Se asume que la query continua en ejecución
                // id_query = worker->query_asignada;
                id_query = recibir_entero(fd_worker, master_logger);
                char* file_tag = recibir_string(fd_worker, master_logger);

                char* string_recibido = recibir_string(fd_worker, master_logger);
                if (!string_recibido)
                {
                    log_error(master_logger, "Error al recibir string de la Query %d en el Worker %d", id_query, worker->id);
                    continue;
                }

                log_info(master_logger, "## Se recibió un READ de la Query %d en el Worker %d: %s", id_query, worker->id, string_recibido);

                // enviar al Query Control
                query_t* query = buscar_y_remover_query_por_id(lista_querys, id_query);
                if (query) {
                    // Log según enunciado: reenvío al Query Control
                    log_info(master_logger, "## Se envía un mensaje de lectura de la Query %d en el Worker %d al Query Control tag:%s", query->id, worker->id, file_tag);

                    enviar_operacion(query->fd_query_control, QUERY_READ_OK);
                    enviar_string(query->fd_query_control, master_logger, file_tag);
                    enviar_string(query->fd_query_control, master_logger, string_recibido);

                    // volver a agregar la query de forma segura (usa la función que protege con mutex)
                    agregar_lista(lista_querys, query);
                } else {
                    log_error(master_logger, "No se encontró la Query id: %d, en Lista Global para enviar READ", id_query);
                }

                free(string_recibido);
                break;
            }
            // atender desalojo de Query Control Exitoso
            case DESALOJO_PC: {
                //log_debug(master_logger, "## Se recibio DESALOJO_PC de la Query %d en el Worker %d al Query Control ", id_query, worker->id);
                id_query = recibir_entero(fd_worker, master_logger);
                pc = recibir_entero(fd_worker, master_logger);
                // sacar la query de exec
                query_t* query = buscar_y_remover_query_por_id(cola_exec, id_query);
                if (query) {
                    query->pc = pc;
                    cambiar_estado_query(query, READY);
                    agregar_lista(cola_ready, query);
                    crear_hilo_aging_para_query(query);
                }
                else {
                    log_error(master_logger, "No se encontró la Query id: %d, en Cola EXEC para desalojar", id_query);
                }

                liberar_worker(worker);
                log_info(master_logger, "## Se desaloja la Query %d (%d) del Worker %d Motivo: PRIORIDAD", query->id, query->prioridad, worker->id);
                sem_post(&sem_replanificar);
                log_debug(master_logger, "replanifica por desalo");
                
                break;
            }
            // caso desalojo el id y el pc
            case -1: {
                // Desconexión del Worker
                id_query = worker->query_asignada;
                log_info(master_logger, "## Se desconecta el Worker %d - Se finaliza la Query %d - Cantidad total de Workers: %d", 
                    worker->id, id_query, obtener_cantidad_workers() - 1);

                // Si estaba ejecutando una query, finalizarla con error y avisar a Query Control
                if (id_query != -1) {
                    query_t* query = buscar_y_remover_query_por_id(cola_exec, id_query);
                    if (query) {
                        cambiar_estado_query(query, EXIT);
                        enviar_operacion(query->fd_query_control, WORKER_DOWN);
                    }
                }

                eliminar_worker_por_id(worker->id);
                sem_post(&sem_replanificar); // Puede haber un worker menos, replanificar
                return NULL; 
                break;
            }
            default:

                log_warning(master_logger, "Operación <%d> desconocida recibida del Worker %d", op, worker->id);
                break;
        }
    }
    pthread_exit(NULL);
}