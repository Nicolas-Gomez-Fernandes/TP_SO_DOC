#include "../include/atender_worker.h"

void *atender_worker_storage(void *arg)
{
    t_worker_info *info = (t_worker_info *)arg;
    int fd = info->fd;

    log_info(storage_logger, "[STORAGE] Atendiendo a Worker %s (fd=%d)", info->worker_id, fd);

    int op;
    while (1)
    {
        op = recibir_operacion(fd);

        // Aplicar retardo de operación
        usleep(retardo_operacion * 1000);

        if (op == -1)
        {
            break;
        }

        switch (op)
        {
        case CREATE_FILE:
        {
            // El worker envía: file_tag (string), query_id (int)
            char *file_tag_str = recibir_string(fd, storage_logger);
            int query_id = recibir_entero(fd, storage_logger);

            char *file_name;
            char *tag_name;
            parse_file_tag(file_tag_str, &file_name, &tag_name);

            op_code resultado = create_file(file_name, tag_name, query_id);

            if (resultado == STORAGE_OK)
            {
                // LOG MINIMO OBLIGATORIO
                log_info(storage_logger, "## %d - File Creado %s", query_id, file_tag_str);
            }
            else
            {
                log_error(storage_logger, "Error CREATE: El File:Tag '%s' ya existe.", file_tag_str);
            }
            enviar_operacion(fd, resultado);

            free(file_tag_str);
            free(file_name);
            free(tag_name);
            break;
        }

        case TRUNCATE_FILE:
        {
            char *file_tag_str = recibir_string(fd, storage_logger);
            int nuevo_tamanio = recibir_entero(fd, storage_logger);
            int query_id = recibir_entero(fd, storage_logger);

            log_debug(storage_logger, "Recibida petición TRUNCATE_FILE desde Worker (fd=%d) con query_id %d ", fd, query_id);
           
            char *file_name;
            char *tag_name;
            parse_file_tag(file_tag_str, &file_name, &tag_name);

            op_code resultado = truncate_file(file_name, tag_name, nuevo_tamanio, query_id);

            if (resultado == STORAGE_OK)
            {
                // LOG MINIMO OBLIGATORIO
                log_info(storage_logger, "## %d - File Truncado %s - Tamaño: %d", query_id, file_tag_str, nuevo_tamanio);
            }
            else if (resultado == STORAGE_ERROR_FILE_INEXISTENTE)
            {
                log_error(storage_logger, "Error TRUNCATE: El File:Tag '%s' no existe.", file_tag_str);
            }
            else if (resultado == STORAGE_ERROR_ESCRITURA_NO_PERMITIDA)
            {
                log_error(storage_logger, "Error TRUNCATE: El File:Tag '%s' está confirmado (COMMITED).", file_tag_str);
            }
            enviar_operacion(fd, resultado);

            free(file_tag_str);
            free(file_name);
            free(tag_name);
            break;
        }

        case WRITE_BLOCK_STORAGE:
        {
            char *file_tag_str = recibir_string(fd, storage_logger);
            int nro_bloque_logico = recibir_entero(fd, storage_logger);
            void *buffer = recibir_buffer(fd, storage_logger, block_size);
            int query_id = recibir_entero(fd, storage_logger);

            log_debug(storage_logger, "Recibida petición WRITE_BLOCK_STORAGE desde Worker (fd=%d) con query_id %d ", fd, query_id);

            char *file_name;
            char *tag_name;
            parse_file_tag(file_tag_str, &file_name, &tag_name);

            log_debug(storage_logger, "Recibida petición WRITE para %s, bloque %d", file_tag_str, nro_bloque_logico);
            op_code resultado = write_block(file_name, tag_name, nro_bloque_logico, buffer, query_id);
            usleep(retardo_acceso_bloque * 1000);

            if (resultado == STORAGE_OK)
            {
                // LOG MINIMO OBLIGATORIO
                log_info(storage_logger, "## %d - Bloque Lógico Escrito %s - Número de Bloque: %d", query_id, file_tag_str, nro_bloque_logico);
                log_debug(storage_logger, "Escritura exitosa. Enviando STORAGE_OK a Worker.");
            }
            else
            {
                // Log de errores específicos
                log_error(storage_logger, "Escritura falló. Enviando código de error %d a Worker.", resultado);
            }
            enviar_operacion(fd, resultado);

            free(file_tag_str);
            free(file_name);
            free(tag_name);
            free(buffer);
            break;
        }

        case READ_BLOCK_STORAGE:
        {
            char *file_tag_str = recibir_string(fd, storage_logger);
            int nro_bloque_logico = recibir_entero(fd, storage_logger);
            int query_id = recibir_entero(fd, storage_logger);
            log_debug(storage_logger, "Recibida petición READ_BLOCK_STORAGE desde Worker (fd=%d) con query_id %d ", fd, query_id);

            char *file_name;
            char *tag_name;
            parse_file_tag(file_tag_str, &file_name, &tag_name);
            void *buffer_lectura = NULL;

            op_code resultado = read_block(file_name, tag_name, nro_bloque_logico, &buffer_lectura, query_id);
            usleep(retardo_acceso_bloque * 1000); // Aplicar retardo de acceso a bloque

            enviar_operacion(fd, resultado);
            if (resultado == STORAGE_OK)
            {
                // LOG MINIMO OBLIGATORIO
                log_info(storage_logger, "## %d - Bloque Lógico Leído %s - Número de Bloque: %d", query_id, file_tag_str, nro_bloque_logico);
                enviar_buffer(fd, storage_logger, buffer_lectura, block_size);
                free(buffer_lectura);
            }

            free(file_tag_str);
            free(file_name);
            free(tag_name);
            break;
        }

        case TAG_FILE:
        {
            char *origen_str = recibir_string(fd, storage_logger);
            char *destino_str = recibir_string(fd, storage_logger);
            int query_id = recibir_entero(fd, storage_logger);

            log_debug(storage_logger, "Recibida petición FILE_TAG desde Worker (fd=%d) con query_id %d ", fd, query_id);

            char *origen_file, *origen_tag, *destino_file, *destino_tag;
            parse_file_tag(origen_str, &origen_file, &origen_tag);
            parse_file_tag(destino_str, &destino_file, &destino_tag);

            op_code resultado = tag_file(origen_file, origen_tag, destino_file, destino_tag, query_id);

            if (resultado == STORAGE_OK)
            {
                // LOG MINIMO OBLIGATORIO
                log_info(storage_logger, "## %d - Tag creado %s", query_id, destino_str);
            }
            else
            {
                // Log de errores
            }
            enviar_operacion(fd, resultado);

            free(origen_str);
            free(destino_str);
            free(origen_file);
            free(origen_tag);
            free(destino_file);
            free(destino_tag);
            break;
        }

        case COMMIT_TAG:
        {
            char *file_tag_str = recibir_string(fd, storage_logger);
            int query_id = recibir_entero(fd, storage_logger);

            log_debug(storage_logger, "Recibida petición COMMIT_TAG desde Worker (fd=%d) con query_id %d ", fd, query_id);

            char *file_name;
            char *tag_name;
            parse_file_tag(file_tag_str, &file_name, &tag_name);

            op_code resultado = commit_tag(file_name, tag_name, query_id);
            if (resultado == STORAGE_OK)
            {
                // LOG MINIMO OBLIGATORIO
                log_info(storage_logger, "## %d - Commit de File:Tag %s", query_id, file_tag_str);
            }
            enviar_operacion(fd, resultado);

            free(file_tag_str);
            free(file_name);
            free(tag_name);
            break;
        }

        case DELETE_TAG:
        {
            char *file_tag_str = recibir_string(fd, storage_logger);
            int query_id = recibir_entero(fd, storage_logger);

            log_debug(storage_logger, "Recibida petición DELETE_TAG desde Worker (fd=%d) con query_id %d ", fd, query_id);

            char *file_name;
            char *tag_name;
            parse_file_tag(file_tag_str, &file_name, &tag_name);

            op_code resultado = delete_tag(file_name, tag_name, query_id);
            if (resultado == STORAGE_OK)
            {
                // LOG MINIMO OBLIGATORIO
                log_info(storage_logger, "## %d - Tag Eliminado %s", query_id, file_tag_str);
            }
            enviar_operacion(fd, resultado);

            free(file_tag_str);
            free(file_name);
            free(tag_name);
            break;
        }

        case GET_FILE_INFO:
        {
            char *file_tag_str = recibir_string(fd, storage_logger);
            int query_id = recibir_entero(fd, storage_logger);

            log_debug(storage_logger, "Recibida petición GET_FILE_INFO desde Worker (fd=%d) con query_id %d ", fd, query_id);

            char *file_name;
            char *tag_name;
            parse_file_tag(file_tag_str, &file_name, &tag_name);

            int tamanio_archivo;
            char *estado_archivo;

            op_code resultado = fs_get_file_info(file_name, tag_name, query_id, &tamanio_archivo, &estado_archivo);

            enviar_operacion(fd, resultado);
            if (resultado == STORAGE_OK)
            {
                enviar_entero(fd, storage_logger, tamanio_archivo);
                enviar_string(fd, storage_logger, estado_archivo);
                free(estado_archivo);
            }

            free(file_tag_str);
            free(file_name);
            free(tag_name);
            break;
        }

        default:
            log_warning(storage_logger, "Mock Storage: operación desconocida %d recibida (fd=%d)", op, fd);
            enviar_operacion(fd, STORAGE_ERROR);
            break;
        }
    }

    pthread_mutex_lock(&g_mutex_worker_count);
    g_worker_count--;
    // LOG MINIMO OBLIGATORIO
    log_info(storage_logger, "## Se desconecta el Worker %s - Cantidad de Workers: %d", info->worker_id, g_worker_count);
    pthread_mutex_unlock(&g_mutex_worker_count);

    free(info->worker_id);
    free(info);
    close(fd);

    pthread_exit(NULL);
}