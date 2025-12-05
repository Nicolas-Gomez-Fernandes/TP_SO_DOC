#include "../include/memoria_interna.h"

void *espacio_memoria_principal;
t_marco *tabla_de_marcos;
t_dictionary *tabla_paginas_global;
int puntero_clock = 0;
pthread_mutex_t mutex_memoria;

//---------------------- INICIALIZACION/CREACION MEMORIA INTERNA ----------------------
void *inicializar_memoria_interna()
{
    pthread_mutex_init(&mutex_memoria, NULL);

    int cant_marcos = tam_memoria / tam_pagina;

    espacio_memoria_principal = malloc(tam_memoria);
    if (espacio_memoria_principal == NULL)
    {
        log_error(worker_logger, "Error al reservar memoria principal");
        return NULL;
    }

    tabla_de_marcos = malloc(cant_marcos * sizeof(t_marco));
    if (tabla_de_marcos == NULL)
    {
        log_error(worker_logger, "Error al reservar tabla de marcos");
        free(espacio_memoria_principal);
        return NULL;
    }

    for (int i = 0; i < cant_marcos; i++)
    {
        liberar_marco(i); // Inicializa todos los marcos como libres
    }

    tabla_paginas_global = dictionary_create();
    if (tabla_paginas_global == NULL)
    {
        log_error(worker_logger, "Error al crear diccionario de tablas de páginas");
        free(tabla_de_marcos);
        free(espacio_memoria_principal);
        return NULL;
    }

    log_info(worker_logger, "[WORKER-MEM] Memoria interna inicializada - Tamaño total: %d bytes, Tamaño página: %d bytes, Cantidad marcos: %d",
             tam_memoria, tam_pagina, cant_marcos);

    return espacio_memoria_principal;
}

t_tabla_de_paginas *crear_tabla_paginas(char *file, char *tag, int cant_paginas)
{
    pthread_mutex_lock(&mutex_memoria);

    char *file_tag_id = string_from_format("%s:%s", file, tag);
    // Verificar si ya existe
    if (dictionary_has_key(tabla_paginas_global, file_tag_id))
    {
        log_warning(worker_logger, "[WORKER-MEM] Ya existe el file:tag %s en memoria", file_tag_id);
        pthread_mutex_unlock(&mutex_memoria);
        free(file_tag_id);
        return NULL;
    }

    t_tabla_de_paginas *nueva_tabla = malloc(sizeof(t_tabla_de_paginas));
    nueva_tabla->file_tag_id = file_tag_id;
    nueva_tabla->cant_paginas = cant_paginas;
    nueva_tabla->paginas = malloc(cant_paginas * sizeof(t_pagina));

    // Inicializar todas las páginas
    for (int i = 0; i < cant_paginas; i++)
    {
        nueva_tabla->paginas[i].bit_presencia = false;
        nueva_tabla->paginas[i].bit_uso = false;
        nueva_tabla->paginas[i].bit_modificado = false;
        nueva_tabla->paginas[i].nro_marco = -1;
        nueva_tabla->paginas[i].timestamp_ultimo_uso = 0;
    }

    dictionary_put(tabla_paginas_global, nueva_tabla->file_tag_id, nueva_tabla);

    pthread_mutex_unlock(&mutex_memoria);

    log_info(worker_logger, "[WORKER-MEM] Creada tabla de páginas para %s con %d páginas",
             file_tag_id, cant_paginas);

    return nueva_tabla;
}

//----------------------- LIBERACION/DESTRUCCION MEMORIA INTERNA -----------------------
// Destruye una tabla de páginas específica
void destruir_tabla_paginas(t_tabla_de_paginas *tabla)
{
    if (tabla)
    {
        for (int i = 0; i < tabla->cant_paginas; i++)
        {
            if (tabla->paginas[i].bit_presencia)
            {
                int nro_marco = tabla->paginas[i].nro_marco;
                char file[256], tag[256];
                sscanf(tabla->file_tag_id, "%255[^:]:%255s", file, tag);
                // LOG MINIMO OBLIGATORIO
                log_info(worker_logger, "Query %d: Se libera el Marco: %d perteneciente al - File: %s - Tag: %s", contexto->query_id, nro_marco, file, tag);
                liberar_marco(nro_marco);
            }
        }
        free(tabla->paginas);
        free(tabla->file_tag_id);
        free(tabla);
    }
}

// Libera la tabla de páginas de un file:tag específico
void liberar_tabla_paginas_file_tag(char *file_tag_id)
{
    pthread_mutex_lock(&mutex_memoria);

    t_tabla_de_paginas *tabla = dictionary_remove(tabla_paginas_global, file_tag_id);
    if (tabla)
    {
        destruir_tabla_paginas(tabla);
        log_info(worker_logger, "[WORKER-MEM] Liberada tabla de páginas para %s", file_tag_id);
    }

    pthread_mutex_unlock(&mutex_memoria);
}

void liberar_memoria_interna()
{
    dictionary_destroy_and_destroy_elements(tabla_paginas_global, (void *)destruir_tabla_paginas);

    // Liberar tabla de marcos
    if (tabla_de_marcos)
    {
        free(tabla_de_marcos);
        tabla_de_marcos = NULL;
    }

    // Liberar espacio de memoria principal
    if (espacio_memoria_principal)
    {
        free(espacio_memoria_principal);
        espacio_memoria_principal = NULL;
    }

    log_info(worker_logger, "[WORKER-MEM] Memoria interna liberada correctamente");
}

void liberar_marco(int nro_marco)
{
    tabla_de_marcos[nro_marco].esta_libre = true;
    tabla_de_marcos[nro_marco].tabla_dueña = NULL;
    tabla_de_marcos[nro_marco].nro_pagina_dueña = -1;
}

//----------------------- FUNCIONES DE MEMORIA INTERNA -----------------------
void aplicar_retardo_memoria()
{
    usleep(retardo_memoria * 1000);
}

int buscar_marco_libre()
{
    pthread_mutex_lock(&mutex_memoria);

    int cant_marcos = tam_memoria / tam_pagina;
    for (int i = 0; i < cant_marcos; i++)
    {
        if (tabla_de_marcos[i].esta_libre)
        {
            pthread_mutex_unlock(&mutex_memoria);
            return i;
        }
    }

    pthread_mutex_unlock(&mutex_memoria);
    return -1;
}

void ocupar_marco(int nro_marco, t_tabla_de_paginas *tabla, int nro_pagina, const char *file, const char *tag)
{
    pthread_mutex_lock(&mutex_memoria);
    tabla_de_marcos[nro_marco].esta_libre = false;
    tabla_de_marcos[nro_marco].tabla_dueña = (struct t_tabla_de_paginas *)tabla;
    tabla_de_marcos[nro_marco].nro_pagina_dueña = nro_pagina;
    pthread_mutex_unlock(&mutex_memoria);

    log_info(worker_logger, "Query %d: Se asigna el Marco: %d a la Página: %d perteneciente al - File: %s - Tag: %s", contexto->query_id, nro_marco, nro_pagina, (char *)file, (char *)tag);
}

t_marco *obtener_marco_de_pagina(char *file_tag_id, int nro_pagina)
{
    t_tabla_de_paginas *tabla = dictionary_get(tabla_paginas_global, file_tag_id);

    if (!tabla->paginas[nro_pagina].bit_presencia)
    {
        log_info(worker_logger, "[WORKER-MEM] Página %d no está en memoria", nro_pagina);
        return NULL;
    }

    int nro_marco = tabla->paginas[nro_pagina].nro_marco;

    // Actualizar metadatos de la página (para algoritmos de reemplazo)
    tabla->paginas[nro_pagina].bit_uso = true;
    tabla->paginas[nro_pagina].timestamp_ultimo_uso = time(NULL);

    return &tabla_de_marcos[nro_marco];
}

t_tabla_de_paginas *obtener_tabla_paginas(char *file, char *tag)
{
    char *file_tag_id = string_from_format("%s:%s", file, tag);

    pthread_mutex_lock(&mutex_memoria);

    if (!dictionary_has_key(tabla_paginas_global, file_tag_id))
    {
        pthread_mutex_unlock(&mutex_memoria);
        free(file_tag_id);
        return NULL;
    }
    t_tabla_de_paginas *tabla = dictionary_get(tabla_paginas_global, file_tag_id);

    pthread_mutex_unlock(&mutex_memoria);

    free(file_tag_id);
    return tabla;
}

bool crear_o_actualizar_tabla_paginas(char *file, char *tag, int tamanio)
{
    char *file_tag_id = string_from_format("%s:%s", file, tag);
    // Si la tabla ya existe (por ej. en un TRUNCATE), la liberamos para crear la nueva.
    if (dictionary_has_key(tabla_paginas_global, file_tag_id))
    {
        liberar_tabla_paginas_file_tag(file_tag_id);
    }
    free(file_tag_id);

    int cant_paginas = (tamanio == 0) ? 0 : ((tamanio - 1) / tam_pagina + 1); // Cálculo corregido para 0 y múltiplos
    t_tabla_de_paginas *tabla = crear_tabla_paginas(file, tag, cant_paginas);

    if (tabla != NULL) {
        tabla->estado = WORK_IN_PROGRESS; // <-- CAMBIO AQUÍ: Inicializar estado
        return true;
    }
    return false;
}

//==============================================================================
//=========================== MANEJO DE PAGE FAULT =============================
//==============================================================================

// Escribe el contenido de una página víctima a Storage si está modificada.
bool escribir_victima_a_storage(int nro_marco)
{
    t_marco *marco_victima = &tabla_de_marcos[nro_marco];
    t_tabla_de_paginas *tabla_victima = (t_tabla_de_paginas *)marco_victima->tabla_dueña;
    int pagina_victima_idx = marco_victima->nro_pagina_dueña;
    t_pagina *pagina_victima = &tabla_victima->paginas[pagina_victima_idx];

    if (pagina_victima->bit_modificado)
    {
        log_info(worker_logger, "La víctima (Marco %d, Página %d de %s) está modificada. Escribiendo a Storage...", nro_marco, pagina_victima_idx, tabla_victima->file_tag_id);
        void *datos_victima = espacio_memoria_principal + (nro_marco * tam_pagina);

        enviar_operacion(fd_conexion_storage, WRITE_BLOCK_STORAGE);
        enviar_string(fd_conexion_storage, worker_logger, (char *)tabla_victima->file_tag_id);
        enviar_entero(fd_conexion_storage, worker_logger, pagina_victima_idx);
        enviar_buffer(fd_conexion_storage, worker_logger, datos_victima, tam_pagina);
        enviar_entero(fd_conexion_storage, worker_logger, contexto->query_id);

        if (recibir_operacion(fd_conexion_storage) != STORAGE_OK)
        {
            log_error(worker_logger, "Error crítico: Storage falló al escribir la página víctima. Abortando.");
            return false;
        }
        aplicar_retardo_memoria();
    }

    // La página desalojada ya no está presente en memoria.
    char file[256];
    char tag[256];
    sscanf(tabla_victima->file_tag_id, "%255[^:]:%255s", file, tag);
    log_info(worker_logger, "Desalojando página %d de %s del marco %d", pagina_victima_idx, tabla_victima->file_tag_id, nro_marco);
    pagina_victima->bit_presencia = false;
    pagina_victima->nro_marco = -1;
    return true;
}

// Busca un marco libre o libera uno usando un algoritmo de reemplazo.
int obtener_marco_disponible(const char *file_tag_entrante, int nro_pagina_entrante)
{
    int nro_marco = buscar_marco_libre();
    if (nro_marco == -1)
    {
        log_debug(worker_logger, "No hay marcos libres. Ejecutando algoritmo de reemplazo (%s)...", algoritmo_reemplazo);
        nro_marco = ejecutar_algoritmo_reemplazo();

        if (nro_marco == -1)
        {
            log_error(worker_logger, "Error crítico: El algoritmo de reemplazo no pudo seleccionar una víctima.");
            return -1;
        }

        // Log obligatorio: Reemplazo de página
        t_marco *marco_victima_info = &tabla_de_marcos[nro_marco];
        t_tabla_de_paginas *tabla_victima_info = (t_tabla_de_paginas *)marco_victima_info->tabla_dueña;
        log_info(worker_logger, "## Query %d: Se reemplaza la página %s/%d por la %s/%d", contexto->query_id, tabla_victima_info->file_tag_id, marco_victima_info->nro_pagina_dueña, file_tag_entrante, nro_pagina_entrante);

        if (!escribir_victima_a_storage(nro_marco))
        {
            return -1;
        }
    }
    return nro_marco;
}

// Pide una página a Storage y la carga en el marco especificado.
bool traer_pagina_de_storage(int nro_marco, const char *file_tag, int nro_pagina)
{
    char file[256];
    char tag[256];
    sscanf(file_tag, "%255[^:]:%255s", file, tag);
    log_info(worker_logger, "Query %d: Memoria Miss File: %s - Tag: %s Pagina: %d", contexto->query_id, file, tag, nro_pagina);
    log_debug(worker_logger, "Solicitando a Storage página %d de %s para cargar en marco %d", nro_pagina, file_tag, nro_marco);

    enviar_operacion(fd_conexion_storage, READ_BLOCK_STORAGE);
    enviar_string(fd_conexion_storage, worker_logger, (char *)file_tag);
    enviar_entero(fd_conexion_storage, worker_logger, nro_pagina);
    enviar_entero(fd_conexion_storage, worker_logger, contexto->query_id);

    op_code respuesta = recibir_operacion(fd_conexion_storage);
    if (respuesta == STORAGE_OK)
    {
        void *datos_pagina = recibir_buffer(fd_conexion_storage, worker_logger, tam_pagina);
        if (datos_pagina == NULL)
        {
            log_error(worker_logger, "Error crítico: Storage no pudo enviar el contenido de la página después de confirmar OK.");
            return false;
        }
        memcpy(espacio_memoria_principal + (nro_marco * tam_pagina), datos_pagina, tam_pagina);
        free(datos_pagina);
    }
    else
    {
        log_error(worker_logger, "Error crítico: Storage devolvió un error al solicitar la página %d.", nro_pagina);
        return false;
    }
    aplicar_retardo_memoria();
    return true;
}

int manejar_page_fault(const char *file_tag, int nro_pagina, t_tabla_de_paginas *tabla)
{
    int nro_marco = obtener_marco_disponible(file_tag, nro_pagina);
    if (nro_marco == -1)
        return -1;

    if (!traer_pagina_de_storage(nro_marco, file_tag, nro_pagina))
        return -1;

    char file[256];
    char tag[256];
    sscanf(file_tag, "%255[^:]:%255s", file, tag);

    ocupar_marco(nro_marco, tabla, nro_pagina, file, tag);
    pthread_mutex_lock(&mutex_memoria);
    tabla->paginas[nro_pagina].bit_presencia = true;
    tabla->paginas[nro_pagina].nro_marco = nro_marco;
    tabla->paginas[nro_pagina].bit_modificado = false;
    tabla->paginas[nro_pagina].bit_uso = true;
    tabla->paginas[nro_pagina].timestamp_ultimo_uso = time(NULL);
    pthread_mutex_unlock(&mutex_memoria);
    
    // Log obligatorio
    log_info(worker_logger, "Query %d: - Memoria Add - File: %s - Tag: %s - Pagina: %d - Marco: %d", contexto->query_id, file, tag, nro_pagina, nro_marco);

    return nro_marco;
}

//==============================================================================
//========================= ALGORITMOS DE REEMPLAZO ============================
//==============================================================================

int ejecutar_algoritmo_lru()
{
    int cant_marcos = tam_memoria / tam_pagina;
    int marco_victima = -1;
    time_t lru_timestamp = time(NULL);

    for (int i = 0; i < cant_marcos; i++)
    {
        if (!tabla_de_marcos[i].esta_libre)
        {
            t_tabla_de_paginas *tabla = (t_tabla_de_paginas *)tabla_de_marcos[i].tabla_dueña;
            int nro_pagina = tabla_de_marcos[i].nro_pagina_dueña;
            time_t pagina_timestamp = tabla->paginas[nro_pagina].timestamp_ultimo_uso;

            if (pagina_timestamp <= lru_timestamp)
            {
                lru_timestamp = pagina_timestamp;
                marco_victima = i;
            }
        }
    }
    if (marco_victima != -1)
    {
        log_info(worker_logger, "[LRU] Marco víctima seleccionado: %d", marco_victima);
    }
    return marco_victima;
}

int ejecutar_algoritmo_clockMod()
{
    int cant_marcos = tam_memoria / tam_pagina;

    while (true)
    {
        // --- PRIMERA PASADA: Buscar (U=0, M=0) ---
        for (int i = 0; i < cant_marcos; i++)
        {
            t_marco *marco_actual = &tabla_de_marcos[puntero_clock];
            t_tabla_de_paginas *tabla = (t_tabla_de_paginas *)marco_actual->tabla_dueña;
            t_pagina *pagina = &tabla->paginas[marco_actual->nro_pagina_dueña];

            if (!pagina->bit_uso && !pagina->bit_modificado)
            {
                log_info(worker_logger, "[CLOCK-M] Víctima encontrada en 1ra pasada: Marco %d (U=0, M=0)", puntero_clock);
                int marco_victima = puntero_clock;
                puntero_clock = (puntero_clock + 1) % cant_marcos;
                return marco_victima;
            }
            // Avanzar puntero para la siguiente iteración
            puntero_clock = (puntero_clock + 1) % cant_marcos;
        }

        // --- SEGUNDA PASADA: Buscar (U=0, M=1) y limpiar bits de uso ---
        for (int i = 0; i < cant_marcos; i++)
        {
            t_marco *marco_actual = &tabla_de_marcos[puntero_clock];
            t_tabla_de_paginas *tabla = (t_tabla_de_paginas *)marco_actual->tabla_dueña;
            t_pagina *pagina = &tabla->paginas[marco_actual->nro_pagina_dueña];

            if (!pagina->bit_uso && pagina->bit_modificado)
            {
                log_info(worker_logger, "[CLOCK-M] Víctima encontrada en 2da pasada: Marco %d (U=0, M=1)", puntero_clock);
                int marco_victima = puntero_clock;
                puntero_clock = (puntero_clock + 1) % cant_marcos;
                return marco_victima;
            }

            // Dar segunda oportunidad: limpiar bit de uso
            pagina->bit_uso = false;

            // Avanzar puntero
            puntero_clock = (puntero_clock + 1) % cant_marcos;
        }
        // Al final de esta segunda vuelta, todas las páginas tendrán U=0
        log_debug(worker_logger, "[CLOCK-M] Fin de una vuelta completa. Todos los bits de uso reseteados. Reiniciando búsqueda.");
    }
}

int ejecutar_algoritmo_reemplazo()
{

    if (strcmp(algoritmo_reemplazo, "LRU") == 0)
    {
        return ejecutar_algoritmo_lru();
    }
    else if (strcmp(algoritmo_reemplazo, "CLOCK-M") == 0)
    {
        return ejecutar_algoritmo_clockMod();
    }
    log_error(worker_logger, "Algoritmo de reemplazo '%s' no reconocido.", algoritmo_reemplazo);
    return -1;
}
