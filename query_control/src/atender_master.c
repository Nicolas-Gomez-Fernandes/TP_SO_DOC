#include "../include/atender_master.h"

void* atender_master(void* args) {
    int op;

    while (1) {
        op = recibir_operacion(fd_conexion_master);

        switch (op) {
            case QUERY_READ_OK: {
                char* file_tag = recibir_string(fd_conexion_master, query_control_logger);
                char* contenido = recibir_string(fd_conexion_master, query_control_logger);
                if (file_tag && contenido) {
                    // ## Lectura realizada: File <File:Tag>, contenido: <CONTENIDO>
                    log_info(query_control_logger, "## Lectura realizada: File %s, contenido: %s", file_tag, contenido);
                    free(file_tag);
                    free(contenido);
                } else {
                    log_error(query_control_logger, "Error recibiendo READ");
                }
                break;
            }
            case END: {
                char* motivo_error = recibir_string(fd_conexion_master, query_control_logger);
                log_info(query_control_logger, "## Query Finalizada - motivo: %s", motivo_error);
                free(motivo_error);
                pthread_exit(NULL);
            }
            case WORKER_DOWN: {
                log_info(query_control_logger, "## Query Finalizada - <WORKER_DOWN>");
                pthread_exit(NULL);
                break;
            }
            case -1: {
                // Desconexión del Master
                log_info(query_control_logger, "Desconexión del Master");
                pthread_exit(NULL);
                break;
            }
            default:
                log_warning(query_control_logger, "Operación <%d> desconocida recibida del Master ", op);
                pthread_exit(NULL);
                break;
        }
    }
    pthread_exit(NULL);
}