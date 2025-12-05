#include "../include/inicializar_query_control.h"

void inicializarQueryControl()
{
    inicializar_configs();
    inicializar_logs();
}
void inicializar_logs()
{
    // 1. Obtener la marca de tiempo
    time_t t = time(NULL);
    struct tm *tm = localtime(&t);
    
    // 2. Formatear la marca de tiempo como parte del nombre del archivo
    // Formato de ejemplo: "YYYYMMDD_HHMMSS"
    char timestamp[16]; 
    strftime(timestamp, sizeof(timestamp), "%H%M%S", tm);
    
    // 3. Construir el nombre de archivo final
    char filename[100];
    strcpy(filename, "query_control_");
    strcat(filename, timestamp);
    strcat(filename, ".log"); // Resultado: "query_control_20251128_212512.log"
    
    // 4. Crear el logger con el nombre de archivo dinámico
    query_control_logger = log_create(filename, "QUERY_CONTROL", true, log_level);
    
    // 5. Verificar la inicialización
    if (query_control_logger == NULL)
    {
        // Nota: En este punto, log_error no funcionará si la creación falló.
        // Lo mejor es imprimir a stderr o usar printf.
        fprintf(stderr, "Ocurrió un error al intentar crear el archivo de logs: %s\n", filename);
        exit(EXIT_FAILURE);
    }
}

void inicializar_configs()
{
    query_control_config = config_create("queryControl.config");

    if (query_control_config == NULL)
    {
        // log_error(query_control_logger, "Ocurrio un error con las configs");
        exit(EXIT_FAILURE);
    }

    ip_master = config_get_string_value(query_control_config, "IP_MASTER");
    puerto_master = config_get_string_value(query_control_config, "PUERTO_MASTER");
    log_level = parse_log_level(config_get_string_value(query_control_config, "LOG_LEVEL"));
}

void conectar_a_master(char* ip, char* puerto, int* fd_destino) {
    log_debug(query_control_logger, "Conectando Query Control con Master");
    *fd_destino = crear_conexion(ip, puerto);
    if (*fd_destino == -1) {
        log_error(query_control_logger, "Error conectando con Master (IP: %s, Puerto: %s)", ip, puerto);
        exit(EXIT_FAILURE);
    }
    log_info(query_control_logger, "## Conexión al Master exitosa. IP: %s, Puerto: %s", ip, puerto);
}

void enviar_handshake_query(int socket_fd, char* archivo_config, char* archivo_query, int prioridad) {
    op_code handshake = HANDSHAKE_QUERY_CONTROL;

    log_debug(query_control_logger, "Iniciando handshake con Master para archivo query: %s", archivo_query);

    enviar_operacion(socket_fd, handshake);

    // enviar_string(socket_fd, query_control_logger, archivo_config);
    enviar_string(socket_fd, query_control_logger, archivo_query);
    enviar_entero(socket_fd, query_control_logger, prioridad);
}

void recibir_handshake(int socket_fd) {
    op_code result = recibir_operacion(socket_fd);

    if (result == HANDSHAKE_OK) {
        log_info(query_control_logger, "Handshake con Master completado");
    } else {
        log_error(query_control_logger, "Handshake rechazado por Master");
        close(socket_fd);
        exit(EXIT_FAILURE);
    }
}

void iniciar_conexion_con_master(char* archivo_config, char* archivo_query, int prioridad) {
    conectar_a_master(ip_master, puerto_master, &fd_conexion_master);

    log_info(query_control_logger, "## Solicitud de ejecución de Query: %s, prioridad: %d", archivo_query, prioridad);

    enviar_handshake_query(fd_conexion_master, archivo_config, archivo_query, prioridad);
    recibir_handshake(fd_conexion_master);
}

void terminar_programa() {
    config_destroy(query_control_config);
    log_destroy(query_control_logger);
    liberar_conexion(fd_conexion_master);
}