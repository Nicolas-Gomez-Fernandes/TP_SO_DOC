#include "../include/controlador_query.h"

// bool desalojar_query_en_proceso = false;
// bool query_cancelada = false;

void manejar_interrupcion_query(int fd_master)
{
    int queryId;
    int queryPc;
    if (contexto->interrupcion == CANCELACION)
    {
        queryId=contexto->query_id;
        log_info(worker_logger, "## Query %d: Finalizada por pedido del Master (QUERY_DOWN)", queryId);
        liberar_contexto();
        enviar_operacion(fd_master, QUERY_DOWN);
        enviar_entero(fd_master, worker_logger, queryId);
    }
    if (contexto->interrupcion == DESALOJO)
    {
        queryId=contexto->query_id;
        queryPc=contexto->pc;
        log_info(worker_logger, "## Query %d: Desalojada por pedido del Master", queryId);
        realizar_flush_completo_memoria();
        liberar_contexto();
        enviar_operacion(fd_master, DESALOJO_PC);
        enviar_entero(fd_master, worker_logger, queryId);
        enviar_entero(fd_master, worker_logger, queryPc);
    }
    else
        log_info(worker_logger, "La query ya no tiene contexto activo para desalojar.");
}
