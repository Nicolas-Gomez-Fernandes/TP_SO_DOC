#include "../include/inicializar_master.h"
sem_t sem_replanificar;

void inicializar_master() {
    inicializar_logs();
    inicializar_configs();

    inicializar_gestor_query();
    inicializar_gestor_worker();

    sem_init(&sem_replanificar, 0, 0); // Inicializar el semáforo planificador

    iniciar_servidor_master();
    // crear un hilo para atender conexiones entrantes
    pthread_t hilo_conexiones;
    pthread_create(&hilo_conexiones, NULL, (void*)atender_conexiones_master, NULL);
    pthread_detach(hilo_conexiones);
}

void inicializar_logs()
{
    master_logger = log_create("master_logger.log", "master_logger.log", true, log_level);
    if (master_logger == NULL)
    {
        log_error(master_logger, "Ocurrió un error con el master_logger");
        exit(EXIT_FAILURE);
    }
}

void inicializar_configs()
{
    master_config = config_create("master.config");

    if (master_config == NULL)
    {
        log_error(master_logger, "Ocurrio un error con las configs");
        exit(EXIT_FAILURE);
    }

    puerto_escucha = config_get_string_value(master_config, "PUERTO_ESCUCHA");
    algoritmo_planificacion = config_get_string_value(master_config, "ALGORITMO_PLANIFICACION");
    log_info(master_logger, "algoritmo_planificacion: %s", algoritmo_planificacion);
    tiempo_again = config_get_int_value(master_config, "TIEMPO_AGING");
    log_level = parse_log_level(config_get_string_value(master_config, "LOG_LEVEL"));
}

void iniciar_servidor_master()
{
    fd_escucha = iniciar_servidor(puerto_escucha, master_logger, "MASTER INICIADO");
    if (fd_escucha == -1)
    {
        log_error(master_logger, "[MEMORIA] Error al iniciar servidor en puerto %s", puerto_escucha);
        exit(EXIT_FAILURE);
    }
    log_debug(master_logger, "[MEMORIA] Servidor escuchando en puerto %s", puerto_escucha);
}

void* atender_conexiones_master()
{
    op_code respuestaOk = HANDSHAKE_OK;
    op_code respuestaError = HANDSHAKE_ERROR;

    while (1)
    {
        int fd_cliente = esperar_cliente(fd_escucha, master_logger, "Nueva conexión entrante");
        if (fd_cliente < 0)
            continue;

        op_code handshake = recibir_operacion(fd_cliente);

        //pthread_t hilo;
        int *arg = malloc(sizeof(int));
        *arg = fd_cliente;

        switch (handshake) {
            case HANDSHAKE_QUERY_CONTROL:
                {
                    // Recibir los 3 parámetros: path, priority, query_id
                    char* query_path = recibir_string(fd_cliente, master_logger);
                    int priority = recibir_entero(fd_cliente, master_logger);

                    if (query_path != NULL && priority != -1) {
                        // Crear y almacenar la query real
                        query_t* nueva_query = crear_query(query_path, priority, fd_cliente);
                        log_info(master_logger, "Query creada y almacenada con ID interno %d", nueva_query->id);
                        
                        int cant_querys = obtener_cantidad_querys(); 

                        // Log Obligatorio de Conexión de Query Control
                        log_info(master_logger, "## Se conecta un Query Control para ejecutar la Query %s con prioridad %d Id asignado: %d. Nivel multiprocesamiento %d", query_path, priority, nueva_query->id, cant_querys);
                        enviar_operacion(fd_cliente, respuestaOk);

                        // Crear hilo para atender al Worker
                        pthread_t hiloQuery;
                        pthread_create(&hiloQuery, NULL, atender_query, (void*)nueva_query);
                        pthread_detach(hiloQuery);


                    } else {
                        log_error(master_logger, "Error recibiendo parámetros de Query Control (fd: %d)", fd_cliente);
                        enviar_operacion(fd_cliente, respuestaError);
                        close(fd_cliente);
                        free(arg);
                    }

                    free(query_path);
                }
                break;

            case HANDSHAKE_WORKER:
                {
                    // Asumir que Worker envía un ID o nombre
                    char* worker_id = recibir_string(fd_cliente, master_logger);

                    if (worker_id != NULL) {
                        log_info(master_logger, "Conexión de Worker aceptada - ID: %s (fd: %d)", worker_id, fd_cliente);
                        enviar_operacion(fd_cliente, respuestaOk);

                        // Crear y almacenar el Worker real
                        int id_worker = atoi(worker_id); 
                        worker_t* worker = crear_worker(id_worker, fd_cliente);
                        log_info(master_logger, "Worker creado y almacenado con ID interno %d", id_worker);

                        // Crear hilo para atender al Worker
                        pthread_t hiloWorker;
                        pthread_create(&hiloWorker, NULL, atender_worker, (void*)worker);
                        pthread_detach(hiloWorker);

                    } else {
                        log_error(master_logger, "Error recibiendo ID de Worker (fd: %d)", fd_cliente);
                        enviar_operacion(fd_cliente, respuestaError);
                        close(fd_cliente);
                        free(arg);
                    }

                    free(worker_id);
                }
                break;

            default:
                log_error(master_logger, "Handshake desconocido (%d), cerrando fd=%d", handshake, fd_cliente);
                enviar_operacion(fd_cliente, respuestaError);
                close(fd_cliente);
                free(arg);
                break;
        }
    }
}

void terminar_programa() {
    config_destroy(master_config);
    log_destroy(master_logger);
    destruir_gestor_query();
    destruir_gestor_worker();
    // liberar_conexion(fd_escucha);
    // liberar_conexion(fd_cliente_query_control);
    // liberar_conexion(fd_cliente_worker);
}