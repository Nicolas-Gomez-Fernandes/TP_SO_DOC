#include "../include/inicializar_storage.h"

// Definición de variables globales
char* puerto_storage;
char* fresh_start;
char* punto_montaje;
int retardo_operacion;
int retardo_acceso_bloque;
t_log_level log_level;
t_log* storage_logger;
t_config* storage_config;
t_config* superblock_config;
int socket_servidor;

void inicializar_storage(char* path)
{
    // 1. Leer la configuración y luego iniciar los logs.
    inicializar_configs(path);
    inicializar_logs();

    // 3. Inicializar las estructuras del File System (formatear o cargar).
    inicializar_filesystem(punto_montaje, fresh_start);

    // 4. Con la configuración y el FS listos, iniciamos el servidor.
    iniciar_servidor_storage();
    // 5. Y finalmente, nos ponemos a atender las conexiones.
    atender_conexiones_storage();
}

void inicializar_logs()
{
    // Primero parseamos el nivel de log desde el config
    char* log_level_str = config_get_string_value(storage_config, "LOG_LEVEL");
    if (log_level_str == NULL) {
        // Valor por defecto si no se encuentra
        log_level = LOG_LEVEL_INFO;
    } else {
        log_level = log_level_from_string(log_level_str);
    }
    storage_logger = log_create("storage_logger.log", "Storage", true, log_level);
    if (storage_logger == NULL)
    {
        log_error(storage_logger, "Ocurrió un error con el storage_logger");
        exit(EXIT_FAILURE);
    }
}

void inicializar_configs(char* path)
{
    storage_config = config_create(path);

    puerto_storage = config_get_string_value(storage_config, "PUERTO_ESCUCHA");
    fresh_start = config_get_string_value(storage_config, "FRESH_START");
    punto_montaje = config_get_string_value(storage_config, "PUNTO_MONTAJE");
    retardo_operacion = config_get_int_value(storage_config, "RETARDO_OPERACION");
    retardo_acceso_bloque = config_get_int_value(storage_config, "RETARDO_ACCESO_BLOQUE");
    log_level = log_level_from_string(config_get_string_value(storage_config, "LOG_LEVEL"));
}

void iniciar_servidor_storage()
{
    log_info(storage_logger, "Iniciando servidor Storage");
    socket_servidor = iniciar_servidor(puerto_storage, storage_logger, "Storage iniciado");
    if (socket_servidor == -1)
    {
        log_error(storage_logger, "Error iniciando servidor Storage");
        exit(EXIT_FAILURE);
    }
    log_debug(storage_logger, "Storage escuchando en puerto %s", puerto_storage);
}

void atender_conexiones_storage()
{
    while (1)
    {
        int* cliente_fd_ptr = malloc(sizeof(int));
        *cliente_fd_ptr = esperar_cliente(socket_servidor, storage_logger, "Nueva conexión entrante");

        if (*cliente_fd_ptr != -1)
        {
            op_code code_op = recibir_operacion(*cliente_fd_ptr);
            if (code_op == HANDSHAKE_STORAGE)
            {
                char *worker_id = recibir_string(*cliente_fd_ptr, storage_logger);
                if (worker_id != NULL)
                {
                    t_worker_info* info = malloc(sizeof(t_worker_info));
                    info->fd = *cliente_fd_ptr;
                    info->worker_id = strdup(worker_id);
                    free(cliente_fd_ptr); 

                    pthread_mutex_lock(&g_mutex_worker_count);
                    g_worker_count++;
                    // LOG MINIMO OBLIGATORIO
                    log_info(storage_logger, "## Se conecta el Worker %s - Cantidad de Workers: %d", info->worker_id, g_worker_count);
                    pthread_mutex_unlock(&g_mutex_worker_count);
                    
                    enviar_operacion(info->fd, HANDSHAKE_OK);
                    enviar_entero(info->fd, storage_logger, fs_size);
                    enviar_entero(info->fd, storage_logger, block_size);
                    
                    // Crear hilo para atender al worker de forma concurrente
                    pthread_t hilo_worker;
                    pthread_create(&hilo_worker, NULL, (void*)atender_worker_storage, (void*)info);
                    pthread_detach(hilo_worker); 
                    
                    free(worker_id);
                }
                else
                {
                    log_error(storage_logger, "Error recibiendo ID de Worker");
                    enviar_operacion(*cliente_fd_ptr, HANDSHAKE_ERROR);
                    close(*cliente_fd_ptr);
                    free(cliente_fd_ptr);
                }
            }
            else
            {
                log_error(storage_logger, "Handshake inválido de cliente (fd: %d)", *cliente_fd_ptr);
                enviar_operacion(*cliente_fd_ptr, HANDSHAKE_ERROR);
                close(*cliente_fd_ptr);
                free(cliente_fd_ptr);
            }
        } else {
            free(cliente_fd_ptr); 
        }
    }
}

void terminar_programa()
{
    config_destroy(storage_config);
    log_destroy(storage_logger);
    close(socket_servidor);
    destruir_filesystem();
}