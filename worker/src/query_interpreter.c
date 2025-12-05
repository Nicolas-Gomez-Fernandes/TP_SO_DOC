#include "../include/query_interpreter.h"

// Esta es ahora la función principal que contiene el ciclo de ejecución
void run_query_cycle()
{
    t_opcode opcode = INVALID; 

    while (contexto->pc < list_size(contexto->instrucciones) && !contexto->interrupcion)
    {
        char *linea = list_get(contexto->instrucciones, contexto->pc);

        char *linea_para_log = strdup(linea);
        char *opcode_str_log = strtok(linea_para_log, " ");
        log_info(worker_logger, "## Query %d: FETCH Program Counter: %d %s", contexto->query_id, contexto->pc, opcode_str_log);
        free(linea_para_log);

        // Parsear y ejecutar
        opcode = parse_opcode(linea);
        if (opcode == INVALID) {
            log_error(worker_logger, "Query %d: Instrucción inválida en PC %d: %s", contexto->query_id, contexto->pc, linea);
            notificar_master_fin_query(contexto->query_id, "ERROR INSTRUCCION INVALIDA"); 
            break;
        }

        int param_count = 0;
        char **params = parse_params(linea, &param_count);

        bool exito = ejecutar_instruccion(opcode, params, param_count);
        liberar_params(params, param_count); // Liberar params después de usar
        
        if (!exito || opcode == END_OP) break;
        
        // --- Instrucción Realizada ---
        linea_para_log = strdup(linea);
        opcode_str_log = strtok(linea_para_log, " ");
        log_info(worker_logger, "## Query %d: Instrucción realizada: %s", contexto->query_id, opcode_str_log);
        free(linea_para_log);

        contexto->pc++;

        if (contexto->interrupcion != NINGUNA_INTERRUPCION) {
            manejar_interrupcion_query(fd_conexion_master);
            return;
        }
    }
    if (contexto->pc >= list_size(contexto->instrucciones) || (opcode == END_OP))
        notificar_master_fin_query(contexto->query_id, NULL); 

    // Resetear contexto global
    liberar_contexto();
}

//====================================
//======= FUNCIONES AUXILIARES =======
//====================================
t_list *cargar_instrucciones(const char *full_path)
{
    t_list *lista = list_create();
    FILE *file = fopen(full_path, "r");
    if (file == NULL)
    {
        log_error(worker_logger, "Error abriendo archivo de Query: %s", full_path);
        list_destroy(lista);
        return NULL;
    }
    
    char *linea = NULL;
    size_t len = 0;
    int linea_num = 0;
    
    while (getline(&linea, &len, file) != -1)
    {
        linea_num++;
        
        // Limpiar línea
        char* linea_limpia = string_duplicate(linea);
        string_trim(&linea_limpia);
        
        if (strlen(linea_limpia) > 0)
        {
            // Log simple y seguro
            log_trace(worker_logger, "Instrucción %d cargada", linea_num);
            list_add(lista, linea_limpia);
        } else {
            free(linea_limpia);
        }
    }
    
    free(linea);
    fclose(file);
    
    log_debug(worker_logger, "Cargadas %d instrucciones desde %s", list_size(lista), full_path);
    return lista;
}

t_opcode parse_opcode(const char *linea)
{
    char *copia = strdup(linea);
    char *primera_palabra = strtok(copia, " ");
    if (primera_palabra == NULL)
    {
        free(copia);
        return INVALID;
    }
    t_opcode opcode = INVALID;
    if (string_equals_ignore_case(primera_palabra, "CREATE"))   opcode = CREATE;
    else if (string_equals_ignore_case(primera_palabra, "TRUNCATE")) opcode = TRUNCATE;
    else if (string_equals_ignore_case(primera_palabra, "WRITE"))    opcode = WRITE;
    else if (string_equals_ignore_case(primera_palabra, "READ"))     opcode = READ;
    else if (string_equals_ignore_case(primera_palabra, "TAG"))      opcode = TAG;
    else if (string_equals_ignore_case(primera_palabra, "COMMIT"))   opcode = COMMIT;
    else if (string_equals_ignore_case(primera_palabra, "FLUSH"))    opcode = FLUSH;
    else if (string_equals_ignore_case(primera_palabra, "DELETE"))   opcode = DELETE;
    else if (string_equals_ignore_case(primera_palabra, "END"))      opcode = END_OP;
    free(copia);
    return opcode;
}

char **parse_params(const char *linea, int *param_count)
{
    *param_count = 0;
    char **params = NULL;
    char *copia = strdup(linea);
    char *token = strtok(copia, " "); // opcode

    // Para WRITE, el tercer parámetro es todo lo que queda
    bool es_write = (token != NULL && string_equals_ignore_case(token, "WRITE"));

    token = strtok(NULL, " "); // primer parámetro
    while (token != NULL)
    {
        (*param_count)++;
        params = realloc(params, sizeof(char *) * (*param_count));

        if (es_write && *param_count == 3) {
            // strstr(linea, token) encuentra la primera ocurrencia de `token` en la línea original.
            // Esto asegura que tomamos todo el resto, incluyendo espacios.
            char* resto_de_linea = strstr(linea, token);
            params[*param_count - 1] = strdup(resto_de_linea);
            break; // Salimos del bucle
        } else {
            params[*param_count - 1] = strdup(token);
            token = strtok(NULL, " ");
        }
    }
    free(copia);
    return params;
}


void liberar_params(char **params, int param_count)
{
    if (params == NULL) return;
    for (int i = 0; i < param_count; i++)
    {
        free(params[i]);
    }
    free(params);
}

bool ejecutar_instruccion(t_opcode opcode, char **params, int param_count)
{
    switch (opcode)
    {
    case CREATE:   return execute_create(params, param_count);
    case TRUNCATE: return execute_truncate(params, param_count);
    case WRITE:    return execute_write(params, param_count);
    case READ:     return execute_read(params, param_count);
    case TAG:      return execute_tag(params, param_count);
    case COMMIT:   return execute_commit(params, param_count);
    case FLUSH:    return execute_flush(params, param_count);
    case DELETE:   return execute_delete(params, param_count);
    case END_OP:   return execute_end_op(params, param_count);
    default:
        log_error(worker_logger, "Query %d: Opcode inválido en ejecución", contexto->query_id);
        return false;
    }
}

void notificar_master_fin_query(int query_id, const char* motivo_error_str)
{
    if (motivo_error_str == NULL) {
        // Finalización correcta
        enviar_operacion(fd_conexion_master, QUERY_FINALIZADA);
        enviar_entero(fd_conexion_master, worker_logger, query_id);
        log_info(worker_logger, "Query %d: Notificando al Master finalización correcta.", query_id);
    } else {
        // Finalización con error
        enviar_operacion(fd_conexion_master, QUERY_ERROR);
        enviar_entero(fd_conexion_master, worker_logger, query_id);
        enviar_string(fd_conexion_master, worker_logger, (char*)motivo_error_str);
        log_info(worker_logger, "Query %d: Notificando al Master finalización con error (Motivo: %s).", query_id, motivo_error_str);
    }
}


void liberar_contexto()
{
    if (contexto == NULL) return;
    
    // 1. Limpiar y liberar la lista de instrucciones
    if (contexto->instrucciones != NULL) 
        list_destroy_and_destroy_elements(contexto->instrucciones, free);
    
    // 2. Liberar la estructura principal (t_query_context)
    free(contexto);
    
    // 3. Poner el puntero global a NULL para evitar punteros colgantes
    contexto = NULL;

    log_debug(worker_logger,"contexto = NULL;");
}