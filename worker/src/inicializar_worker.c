#include "../include/inicializar_worker.h"

int fd_conexion_master;
int fd_conexion_storage;

char *ip_master;
char *puerto_master;
char *ip_storage;
char *puerto_storage;
int tam_memoria;
int retardo_memoria;
char *algoritmo_reemplazo;
char *path_query;

t_log_level log_level;
t_log *worker_logger;
t_config *worker_config;
char *worker_id = "1";
char *archivo_config = "worker.config";

t_list *instrucciones;
int pc;
int current_query_id;
t_query_context *contexto;
int tam_pagina;
int fs_size;
sem_t sem_procesamiento;

void inicializar_worker(char *archivo_config_path)
{
    inicializar_logs();
    inicializar_configs(archivo_config_path);

    sem_init(&sem_procesamiento, 0, 1);
}

void inicializar_logs()
{
    char nombre_log[64];
    sprintf(nombre_log, "worker_%s.log", worker_id);
    worker_logger = log_create(nombre_log, "WORKER", true, log_level);

    if (worker_logger == NULL)
    {
        fprintf(stderr, "No se pudo crear el logger de WORKER\n");
        exit(EXIT_FAILURE);
    }

    log_debug(worker_logger, "WORKER_LOGGER inicializado con ID: %s", worker_id);
}

void inicializar_configs(char *archivo_config_path)
{
    worker_config = config_create(archivo_config_path);
    if (worker_config == NULL)
    {
        log_error(worker_logger, "Ocurrio un error con las configs: %s", archivo_config_path);
        exit(EXIT_FAILURE);
    }
    ip_master = config_get_string_value(worker_config, "IP_MASTER");
    puerto_master = config_get_string_value(worker_config, "PUERTO_MASTER");
    ip_storage = config_get_string_value(worker_config, "IP_STORAGE");
    puerto_storage = config_get_string_value(worker_config, "PUERTO_STORAGE");
    tam_memoria = config_get_int_value(worker_config, "TAM_MEMORIA");
    retardo_memoria = config_get_int_value(worker_config, "RETARDO_MEMORIA");
    algoritmo_reemplazo = config_get_string_value(worker_config, "ALGORITMO_REEMPLAZO");
    path_query = config_get_string_value(worker_config, "PATH_SCRIPTS");
    log_debug(worker_logger, "Path de queries: %s\n", path_query);
    log_level = parse_log_level(config_get_string_value(worker_config, "LOG_LEVEL"));
}

void conectar_a_master(char *ip, char *puerto, int *fd_destino)
{
    log_info(worker_logger, "Conectando Worker con Master (Puerto: %s)", puerto);
    *fd_destino = crear_conexion(ip, puerto);
    if (*fd_destino == -1)
    {
        log_error(worker_logger, "Error conectando con Master");
        exit(EXIT_FAILURE);
    }
    log_info(worker_logger, "Conexión con Master establecida (fd: %d)", *fd_destino);
}

void conectar_a_storage(char *ip, char *puerto, int *fd_destino)
{
    log_info(worker_logger, "Conectando Worker con Storage (Puerto: %s)", puerto);
    *fd_destino = crear_conexion(ip, puerto);
    if (*fd_destino == -1)
    {
        log_error(worker_logger, "Error conectando con Storage");
        exit(EXIT_FAILURE);
    }
    log_info(worker_logger, "Conexión con Storage establecida (fd: %d)", *fd_destino);
}

void enviar_handshake_worker_master(int socket_fd, char *worker_id)
{
    op_code handshake = HANDSHAKE_WORKER;
    log_debug(worker_logger, "Iniciando handshake con Master (ID: %s)", worker_id);
    enviar_operacion(socket_fd, handshake);
    enviar_string(socket_fd, worker_logger, worker_id);
}

void enviar_handshake_worker_storage(int socket_fd, char *worker_id)
{
    op_code handshake = HANDSHAKE_STORAGE;
    log_debug(worker_logger, "Iniciando handshake con Storage (ID: %s)", worker_id);
    enviar_operacion(socket_fd, handshake);
    enviar_string(socket_fd, worker_logger, worker_id);
}

void recibir_handshake(int socket_fd, char *modulo)
{
    op_code result = recibir_operacion(socket_fd);
    if (result == HANDSHAKE_OK)
    {
        if (strcmp(modulo, "Storage") == 0)
        {
            fs_size = recibir_entero(socket_fd, worker_logger);
            tam_pagina = recibir_entero(socket_fd, worker_logger);
            log_info(worker_logger, "Handshake con Storage completado - FS Size: %d, Tamaño de página: %d", fs_size, tam_pagina);
        }
        else
            log_info(worker_logger, "Handshake con %s completado", modulo);
    }
    else
    {
        log_error(worker_logger, "Handshake rechazado por %s", modulo);
        close(socket_fd);
        exit(EXIT_FAILURE);
    }
}

void iniciar_conexiones_iniciales(char *worker_id)
{
    conectar_a_storage(ip_storage, puerto_storage, &fd_conexion_storage);
    enviar_handshake_worker_storage(fd_conexion_storage, worker_id);
    recibir_handshake(fd_conexion_storage, "Storage");

    if (inicializar_memoria_interna() == NULL)
    {
        log_error(worker_logger, "No se pudo inicializar la memoria interna. Abortando worker.");
        exit(EXIT_FAILURE);
    }

    conectar_a_master(ip_master, puerto_master, &fd_conexion_master);
    enviar_handshake_worker_master(fd_conexion_master, worker_id);
    recibir_handshake(fd_conexion_master, "Master");
}

pthread_t hilo_query_execution;
void *ejecutar_query_thread()
{
    run_query_cycle();

    sem_post(&sem_procesamiento);
    return NULL;
}

void esperar_instrucciones_master()
{
    while (1)
    {
        op_code operacion = recibir_operacion(fd_conexion_master);

        switch (operacion)
        {
        case ASIGNAR_QUERY:
                // NUEVO: Esperamos a que el slot esté libre.
                // Si el hilo anterior está terminando, esto se bloqueará unos milisegundos 
                // hasta que haga el sem_post, evitando el error de concurrencia.
                sem_wait(&sem_procesamiento);

                current_query_id = recibir_entero(fd_conexion_master, worker_logger);
                char *relative_path = recibir_string(fd_conexion_master, worker_logger);
                pc = recibir_entero(fd_conexion_master, worker_logger);

                /*
                if (contexto != NULL) {
                    log_error(worker_logger, "Error: Se recibió una nueva asignación de query mientras otra estaba en proceso.");
                    notificar_master_fin_query(current_query_id, "ERROR: YA HAY UNA QUERY EN PROCESO");
                    free(relative_path);
                    continue;
                }
                */

                // --- Preparación de la Query ---
                char *full_path = string_new();
                string_append_with_format(&full_path, "%s/%s", path_query, relative_path);
                log_info(worker_logger, "## Query %d: Se recibe la Query. El path de operaciones es: %s", current_query_id, full_path);
                instrucciones = cargar_instrucciones(full_path);
                if (instrucciones == NULL || list_is_empty(instrucciones)) {
                    log_error(worker_logger, "Error cargando instrucciones");
                    notificar_master_fin_query(current_query_id, "ERROR CARGA INSTRUCCIONES");
                    free(full_path);
                    free(relative_path);

                    sem_post(&sem_procesamiento);
                    continue;
                }
                inicializar_contexto(current_query_id, pc, instrucciones);

                // --- Ejecución de la Query ---
                pthread_create(&hilo_query_execution, NULL, ejecutar_query_thread, NULL);
                pthread_detach(hilo_query_execution);

                free(full_path);
                free(relative_path);
                break;
            
        case DESALOJAR_QUERY:
                int id_a_desalojar = recibir_entero(fd_conexion_master, worker_logger);
                log_info(worker_logger, "Recibida solicitud de DESALOJO desde Master para query %i.", id_a_desalojar);
                if (contexto) contexto->interrupcion = DESALOJO;
                break;

        case QUERY_DOWN:
            int id_a_cancelar = recibir_entero(fd_conexion_master, worker_logger);
            log_info(worker_logger, "Recibida solicitud de CANCELACIÓN (QUERY_DOWN) para query %i.", id_a_cancelar);
            if (contexto) contexto->interrupcion = CANCELACION;   
            break;
        default:
            log_warning(worker_logger, "Operacion desconocida de Master: %d", operacion);
            return;
            break;
        }
    }
}

void inicializar_contexto(int query_id, int initial_pc, t_list *instrucciones_cargadas)
{
    // Si ya existía un contexto de una query anterior, lo liberamos
    if (contexto != NULL)
        liberar_contexto();
    
    contexto = malloc(sizeof(t_query_context));
    contexto->query_id = query_id;
    contexto->pc = initial_pc;
    contexto->interrupcion = NINGUNA_INTERRUPCION;
    contexto->instrucciones = instrucciones_cargadas;

    log_info(worker_logger, "Contexto creado para Query %d con PC: %d", query_id, initial_pc);
}

void terminar_programa()
{
    config_destroy(worker_config);
    log_destroy(worker_logger);
    liberar_conexion(fd_conexion_master);
    liberar_conexion(fd_conexion_storage);
}