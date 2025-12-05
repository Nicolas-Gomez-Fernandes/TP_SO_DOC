#include "../include/instrucciones_worker.h"
#include "../include/main.h" 
#include "../include/memoria_interna.h" 

static void parse_file_tag(const char* file_tag_str, char** file, char** tag) {
    char* copia = strdup(file_tag_str);
    *file = strdup(strtok(copia, ":"));
    *tag = strdup(strtok(NULL, ":"));
    free(copia);
}

static const char* op_code_to_string(op_code code) {
    switch(code) {
        case STORAGE_ERROR_FILE_PREEXISTENTE: return "ERROR FILE PREEXISTENTE";
        case STORAGE_ERROR_TAG_INEXISTENTE: return "ERROR TAG INEXISTENTE";
        case STORAGE_ERROR_ESCRITURA_NO_PERMITIDA: return " ERROR ESCRITURA NO PERMITIDA";
        case STORAGE_ERROR_ESPACIO_INSUFICIENTE: return "ERROR ESPACIO BLOQUES INSUFICIENTES";
        case STORAGE_ERROR_ESCRITURA_FUERA_LIMITE: return "ERROR ESCRITURA FUERA DEL LIMITE";
        case STORAGE_ERROR_LECTURA_FUERA_LIMITE: return "ERROR LECTURA FUERA DEL LIMITE";
        case STORAGE_ERROR_DELETE_NO_PERMITIDO: return "ERROR DELETE NO PERMITIDO";
        case QUERY_ERROR: return "QUERY ERROR INTERNO";
        default: return "ERROR DESCONOCIDO";
    }
}
 
static bool manejar_respuesta_storage(const char* operacion_str, const char* file_tag_1, const char* file_tag_2) {
    op_code respuesta = recibir_operacion(fd_conexion_storage);

    if (respuesta == STORAGE_OK) {
        return true;
    }

    // Si hay error, loguear y notificar al master
    switch(respuesta) {
        case STORAGE_ERROR_FILE_PREEXISTENTE:
            log_error(worker_logger, "## Query %d: Error en %s - El File:Tag '%s' ya existe.", contexto->query_id, operacion_str, file_tag_1);
            break;
        case STORAGE_ERROR_TAG_INEXISTENTE:
             log_error(worker_logger, "## Query %d: Error en %s - El File:Tag '%s' no existe.", contexto->query_id, operacion_str, file_tag_1);
            break;
        case STORAGE_ERROR_ESCRITURA_NO_PERMITIDA:
            log_error(worker_logger, "## Query %d: Error en %s - El File:Tag '%s' está confirmado (COMMITED).", contexto->query_id, operacion_str, file_tag_1);
            break;
        case STORAGE_ERROR_ESPACIO_INSUFICIENTE:
            log_error(worker_logger, "## Query %d: Error en %s - Espacio insuficiente en Storage.", contexto->query_id, operacion_str);
            break;
        case STORAGE_ERROR_ESCRITURA_FUERA_LIMITE:
            log_error(worker_logger, "## Query %d: Error en %s - Acceso fuera de los límites del archivo '%s'.", contexto->query_id, operacion_str, file_tag_1);
            break;
        case STORAGE_ERROR_DELETE_NO_PERMITIDO:
            log_error(worker_logger, "## Query %d: Error en %s - No se permite eliminar el archivo protegido '%s'.", contexto->query_id, operacion_str, file_tag_1);
            break;
        default:
            log_error(worker_logger, "## Query %d: Error inesperado del Storage para %s en '%s'.", contexto->query_id, operacion_str, file_tag_1);
            break;
    }
    const char* motivo_str = op_code_to_string(respuesta);
    notificar_master_fin_query(contexto->query_id, motivo_str);
    return false;
}
// =================================================================
// =================== INSTRUCCIONES SIMPLES =======================
// =================================================================

bool execute_create(char **params, int param_count) {
    if (param_count != 1) {
        log_error(worker_logger, "CREATE: Parámetros incorrectos.");
        notificar_master_fin_query(contexto->query_id, "ERROR EN PARAMETROS RECIBIDOS PARA CREATE");
        return false;
    }
    char* file_tag = params[0];

    enviar_operacion(fd_conexion_storage, CREATE_FILE);
    enviar_string(fd_conexion_storage, worker_logger, file_tag);
    enviar_entero(fd_conexion_storage, worker_logger, contexto->query_id);

    if (manejar_respuesta_storage("CREATE", file_tag, NULL)) {
        log_info(worker_logger, "## Query %d: Instrucción CREATE (%s) realizada.", contexto->query_id, file_tag);
        char *file, *tag;
        parse_file_tag(file_tag, &file, &tag);
        crear_o_actualizar_tabla_paginas(file, tag, 0); // Archivo nuevo tiene 0 páginas
        free(file); free(tag);
    }
    else return false;
    aplicar_retardo_memoria();
    return true;
}

bool execute_truncate(char **params, int param_count) {
    if (param_count != 2) {
        log_error(worker_logger, "TRUNCATE: Parámetros incorrectos.");
        notificar_master_fin_query(contexto->query_id, "ERROR EN PARAMETROS RECIBIDOS PARA TRUNCATE");
        return false;
    }
    char* file_tag = params[0];
    int nuevo_tamanio = atoi(params[1]);

    if (nuevo_tamanio % tam_pagina != 0 && nuevo_tamanio != 0) {
        log_error(worker_logger, "## Query %d: Error en TRUNCATE - El tamaño %d no es múltiplo del tamaño de página/bloque (%d).", contexto->query_id, nuevo_tamanio, tam_pagina);
        notificar_master_fin_query(contexto->query_id, "ERROR TAMANIO NO ES MULTIPLO DE LA PAGINA");
        return false;
    }

    enviar_operacion(fd_conexion_storage, TRUNCATE_FILE);
    enviar_string(fd_conexion_storage, worker_logger, file_tag);
    enviar_entero(fd_conexion_storage, worker_logger, nuevo_tamanio);
    enviar_entero(fd_conexion_storage, worker_logger, contexto->query_id);

    if (manejar_respuesta_storage("TRUNCATE", file_tag, NULL)) {
        log_info(worker_logger, "## Query %d: Instrucción TRUNCATE (%s a %d bytes) realizada.", contexto->query_id, file_tag, nuevo_tamanio);        
        char *file, *tag;
        parse_file_tag(file_tag, &file, &tag);
        crear_o_actualizar_tabla_paginas(file, tag, nuevo_tamanio);
        free(file); free(tag);
    }
    else return false;
    aplicar_retardo_memoria();
    return true;
}

bool execute_delete(char **params, int param_count) {
    if (param_count != 1) {
        log_error(worker_logger, "DELETE: Parámetros incorrectos.");
        notificar_master_fin_query(contexto->query_id, "ERROR EN PARAMETROS RECIBIDOS PARA DELETE");
        return false;
    }
    char* file_tag = params[0];

    enviar_operacion(fd_conexion_storage, DELETE_TAG);
    enviar_string(fd_conexion_storage, worker_logger, file_tag);
    enviar_entero(fd_conexion_storage, worker_logger, contexto->query_id);

    if (manejar_respuesta_storage("DELETE", file_tag, NULL)) {
        log_info(worker_logger, "## Query %d: Instrucción DELETE (%s) realizada.", contexto->query_id, file_tag);
        liberar_tabla_paginas_file_tag(file_tag);
    }
    else return false;
    aplicar_retardo_memoria();
    return true;
}


// =================================================================
// =============== INSTRUCCIONES (MEMORIA) ===============
// =================================================================

// Traduce una dirección lógica a una dirección física
static void* traducir_direccion_logica(const char* file_tag, t_tabla_de_paginas* tabla, int direccion_logica) {
    int nro_pagina = direccion_logica / tam_pagina;
    int offset_en_pagina = direccion_logica % tam_pagina;

    if (nro_pagina >= tabla->cant_paginas) {
        log_error(worker_logger, "## Query %d: Segmentation Fault. Dirección lógica %d fuera de los límites del archivo '%s'.", contexto->query_id, direccion_logica, file_tag);
        return NULL;
    }

    if (!tabla->paginas[nro_pagina].bit_presencia) {
        if (manejar_page_fault(file_tag, nro_pagina, tabla) == -1) {
            log_error(worker_logger, "## Query %d: No se pudo manejar el page fault para la página %d de '%s'.", contexto->query_id, nro_pagina, file_tag);
            return NULL;
        }
    }

    int nro_marco = tabla->paginas[nro_pagina].nro_marco;
    return espacio_memoria_principal + (nro_marco * tam_pagina) + offset_en_pagina;
}


static bool gestionar_acceso_paginado(const char* file_tag, int direccion_logica, void* data_buffer, int tamanio, bool es_escritura) {
    char *file, *tag;
    parse_file_tag(file_tag, &file, &tag);
    t_tabla_de_paginas* tabla = obtener_tabla_paginas(file, tag);

    // --- INICIO BUG 2 (WORKER 2): CREAR TABLA BAJO DEMANDA ---
    if (tabla == NULL) {
        log_warning(worker_logger, "[WORKER-MEM] No existe tabla de páginas para %s. Solicitando info a Storage...", file_tag);
        
        enviar_operacion(fd_conexion_storage, GET_FILE_INFO); // Usamos el nuevo op_code
        enviar_string(fd_conexion_storage, worker_logger, (char*)file_tag);
        enviar_entero(fd_conexion_storage, worker_logger, contexto->query_id);
        
        op_code respuesta = recibir_operacion(fd_conexion_storage);
        
        if (respuesta == STORAGE_OK) {
            int tamanio_actual = recibir_entero(fd_conexion_storage, worker_logger);
            char* estado_str = recibir_string(fd_conexion_storage, worker_logger);
            
            log_info(worker_logger, "[WORKER-MEM] Storage informó: %s - Tamaño: %d, Estado: %s", file_tag, tamanio_actual, estado_str);
            
            // Creamos la tabla con la info recibida
            crear_o_actualizar_tabla_paginas(file, tag, tamanio_actual);
            tabla = obtener_tabla_paginas(file, tag); // La obtenemos de nuevo
            
            if (string_equals_ignore_case(estado_str, "COMMITED")) {
                tabla->estado = COMMITED;
            } else {
                tabla->estado = WORK_IN_PROGRESS;
            }
            free(estado_str);

        } else {
            // Si el Storage no nos da OK, es probable que el archivo no exista.
            log_error(worker_logger, "## Query %d: No se pudo obtener información del archivo %s. El archivo no existe.", contexto->query_id, file_tag);
            free(file); 
            free(tag);
            return false;
        }
    }

    free(file); 
    free(tag);

    if (es_escritura && tabla->estado == COMMITED) {
        log_error(worker_logger, "## Query %d: Error en WRITE - El File:Tag '%s' está confirmado (COMMITED).", contexto->query_id, file_tag);
        //notificar_master_fin_query(contexto->query_id, "ERROR ESCRITURA NO PERMITIDA");
        return false; // <-- Simplemente devuelve false
    }

    int bytes_transferidos = 0;
    int dirlogica_actual = direccion_logica;

    while (bytes_transferidos < tamanio) {
        void* direccion_fisica = traducir_direccion_logica(file_tag, tabla, dirlogica_actual);
        if (direccion_fisica == NULL) {
            return false;
        }

        // Calcular cuántos bytes se pueden operar en esta página sin cruzar el limite
        int offset_en_pagina = dirlogica_actual % tam_pagina;
        int bytes_en_pagina = tam_pagina - offset_en_pagina;
        int bytes_restantes = tamanio - bytes_transferidos;
        int bytes_a_copiar = (bytes_en_pagina < bytes_restantes) ? bytes_en_pagina : bytes_restantes;

        if (es_escritura) {
            memcpy(direccion_fisica, data_buffer + bytes_transferidos, bytes_a_copiar);
        } else {
            // CORRECCIÓN de bug de lectura: el destino debe ser el buffer, el origen la memoria física
            memcpy(data_buffer + bytes_transferidos, direccion_fisica, bytes_a_copiar);
        }
        aplicar_retardo_memoria();

        // Log obligatorio
        const char* accion = es_escritura ? "ESCRIBIR" : "LEER";
        log_info(worker_logger, "Query %d: Acción: %s - Dirección Física: %ld - Valor: %.*s", contexto->query_id, accion, (long)(direccion_fisica - espacio_memoria_principal), bytes_a_copiar, (char*)direccion_fisica);

        // Actualizar metadatos
        int nro_pagina = dirlogica_actual / tam_pagina;
        if (es_escritura) {
            tabla->paginas[nro_pagina].bit_modificado = true;
        }
        tabla->paginas[nro_pagina].bit_uso = true;
        tabla->paginas[nro_pagina].timestamp_ultimo_uso = time(NULL);

        bytes_transferidos += bytes_a_copiar;
        dirlogica_actual += bytes_a_copiar;
    }
    return true;
}

bool execute_write(char **params, int param_count) {
    if (param_count != 3) {
        log_error(worker_logger, "WRITE: Parámetros incorrectos. Se esperaban 3.");
        notificar_master_fin_query(contexto->query_id, "ERROR EN PARAMETROS RECIBIDOS PARA WRITE");
        return false;
    }
    char* file_tag = params[0];
    int direccion_logica = atoi(params[1]);
    char* contenido = params[2];
    int bytes_a_escribir = strlen(contenido);

    if (!gestionar_acceso_paginado(file_tag, direccion_logica, contenido, bytes_a_escribir, true)) {
        notificar_master_fin_query(contexto->query_id, "ERROR ACCESO MEMORIA WRITE");
        return false;
    }
    return true;
}

bool execute_read(char **params, int param_count) {
    if (param_count != 3) {
        log_error(worker_logger, "READ: Parámetros incorrectos. Se esperaban 3.");
        notificar_master_fin_query(contexto->query_id, "ERROR EN PARAMETROS RECIBIDOS PARA READ");
        return false;
    }
    char* file_tag = params[0];
    int direccion_logica = atoi(params[1]);
    int tamanio_a_leer = atoi(params[2]);

    char* buffer_lectura = malloc(tamanio_a_leer + 1);
    
    if (gestionar_acceso_paginado(file_tag, direccion_logica, buffer_lectura, tamanio_a_leer, false)) {
        buffer_lectura[tamanio_a_leer] = '\0';
        enviar_operacion(fd_conexion_master, QUERY_READ_OK);
        enviar_entero(fd_conexion_master, worker_logger, contexto->query_id);
        enviar_string(fd_conexion_master, worker_logger, file_tag);
        enviar_string(fd_conexion_master, worker_logger, buffer_lectura);
    } else {
        notificar_master_fin_query(contexto->query_id, "ERROR ACCESO MEMORIA READ");
        free(buffer_lectura);
        return false;
    }
    free(buffer_lectura);
    return true;
}

// =================================================================
// =============== INSTRUCCIONES DE PERSISTENCIA ===================
// =================================================================

bool execute_flush(char **params, int param_count) {
    if (param_count != 1) { 
        log_error(worker_logger, "FLUSH: Parámetros incorrectos.");
        notificar_master_fin_query(contexto->query_id, "ERROR EN PARAMETROS RECIBIDOS PARA FLUSH");
        return false; 
    }
    char* file_tag = params[0];
    
    char *file, *tag;
    parse_file_tag(file_tag, &file, &tag);
    t_tabla_de_paginas* tabla = obtener_tabla_paginas(file, tag);
    free(file); free(tag);

    if (tabla == NULL) { 
        log_error(worker_logger, "FLUSH: No se encontró la tabla de páginas para %s", file_tag);
        notificar_master_fin_query(contexto->query_id, "ERROR TABLA PAGINAS INEXISTENTE");
        return false; 
    }
    
    log_info(worker_logger, "## Query %d: Iniciando FLUSH para %s.", contexto->query_id, file_tag);

    for (int i = 0; i < tabla->cant_paginas; i++) {
        if (tabla->paginas[i].bit_presencia && tabla->paginas[i].bit_modificado) {
            int nro_marco = tabla->paginas[i].nro_marco;
            void* direccion_fisica = espacio_memoria_principal + (nro_marco * tam_pagina);

            enviar_operacion(fd_conexion_storage, WRITE_BLOCK_STORAGE);
            enviar_string(fd_conexion_storage, worker_logger, file_tag);
            enviar_entero(fd_conexion_storage, worker_logger, i);
            enviar_buffer(fd_conexion_storage, worker_logger, direccion_fisica, tam_pagina);
            enviar_entero(fd_conexion_storage, worker_logger, contexto->query_id);

            if (recibir_operacion(fd_conexion_storage) != STORAGE_OK) {
                log_error(worker_logger, "FLUSH: Storage devolvió error al escribir la página %d de %s.", i, file_tag);
                notificar_master_fin_query(contexto->query_id, "ERROR FLUSH STORAGE");
                return false; 
            } else {
                tabla->paginas[i].bit_modificado = false;
                log_debug(worker_logger, "Página %d de %s flusheada a Storage.", i, file_tag);
            }
        }
    }
    aplicar_retardo_memoria();
    return true;
}

bool execute_commit(char **params, int param_count) {
    if (param_count != 1) { 
        log_error(worker_logger, "COMMIT: Parámetros incorrectos.");
        return false; 
    }
    char* file_tag = params[0];

    log_info(worker_logger, "COMMIT: Ejecutando FLUSH implícito para %s.", file_tag);
    if (!execute_flush(params, param_count))
        return false; // Si el flush implícito falla, el commit también.
    
    enviar_operacion(fd_conexion_storage, COMMIT_TAG);
    enviar_string(fd_conexion_storage, worker_logger, file_tag);
    enviar_entero(fd_conexion_storage, worker_logger, contexto->query_id);

    if (manejar_respuesta_storage("COMMIT", file_tag, NULL)) {
        char *file, *tag;
        parse_file_tag(file_tag, &file, &tag);
        t_tabla_de_paginas* tabla = obtener_tabla_paginas(file, tag);
        if (tabla != NULL) {
            tabla->estado = COMMITED;
            log_info(worker_logger, "[WORKER-MEM] Marcada tabla de páginas para %s como COMMITED.", file_tag);
        }
        free(file); free(tag);
        
    }
    else return false;
    
    aplicar_retardo_memoria();
    return true;
}

bool execute_tag(char **params, int param_count)
{
    if (param_count != 2) {
        log_error(worker_logger, "TAG: Cantidad de parámetros incorrecta. Se esperaban 2 (origen y destino).");
        notificar_master_fin_query(contexto->query_id, "ERROR EN PARAMETROS RECIBIDOS PARA TAG");
        return false;
    }

    char* origen_file_tag = params[0];
    char* destino_file_tag = params[1];

    // 1. Enviar solicitud a Storage
    enviar_operacion(fd_conexion_storage, TAG_FILE);
    enviar_string(fd_conexion_storage, worker_logger, origen_file_tag);
    enviar_string(fd_conexion_storage, worker_logger, destino_file_tag);
    enviar_entero(fd_conexion_storage, worker_logger, contexto->query_id);

    // 2. Esperar respuesta de Storage
    if (manejar_respuesta_storage("TAG", origen_file_tag, destino_file_tag)) {
        // Nota: No es necesario crear una tabla de páginas aquí. Se creará si se
        // realiza un TRUNCATE, o sus páginas se cargarán bajo demanda con READ/WRITE.
    }
    else return false;
    aplicar_retardo_memoria();
    return true;
}

bool execute_end_op(char **params, int param_count){ return true; }


void iterador_guardar_paginas_modificadas(char *key, void *value) {
    t_tabla_de_paginas *tabla = (t_tabla_de_paginas *)value;

    for (int i = 0; i < tabla->cant_paginas; i++) {
        
        // Que esté en RAM (Presencia) Y que haya sido modificada (Dirty)
        if (tabla->paginas[i].bit_presencia && tabla->paginas[i].bit_modificado) {
            
            int nro_marco = tabla->paginas[i].nro_marco;
            void *datos_pagina = espacio_memoria_principal + (nro_marco * tam_pagina);

            log_info(worker_logger, "[WORKER-DESALOJO] Guardando cambios pendientes: %s - Pag %d (Marco %d)", tabla->file_tag_id, i, nro_marco);

            // 1. Enviar petición de escritura al Storage
            enviar_operacion(fd_conexion_storage, WRITE_BLOCK_STORAGE);
            enviar_string(fd_conexion_storage, worker_logger, tabla->file_tag_id);
            enviar_entero(fd_conexion_storage, worker_logger, i); // Nro Bloque Lógico
            enviar_buffer(fd_conexion_storage, worker_logger, datos_pagina, tam_pagina);
            enviar_entero(fd_conexion_storage, worker_logger, contexto->query_id);

            // 2. Esperar confirmación para asegurar persistencia 
            op_code respuesta = recibir_operacion(fd_conexion_storage);
            
            if (respuesta == STORAGE_OK) {
                tabla->paginas[i].bit_modificado = false; 
            } else {
                log_error(worker_logger, "Error crítico al persistir %s Pag %d durante desalojo", tabla->file_tag_id, i);
            }

        }
    }
}

void realizar_flush_completo_memoria() 
{
    pthread_mutex_lock(&mutex_memoria);

    if (tabla_paginas_global != NULL && !dictionary_is_empty(tabla_paginas_global)) {
        log_info(worker_logger, "Ejecutando Flush antes del desalojo...");
        dictionary_iterator(tabla_paginas_global, iterador_guardar_paginas_modificadas);
        log_info(worker_logger, "Flush  finalizado.");
    }

    pthread_mutex_unlock(&mutex_memoria);
}