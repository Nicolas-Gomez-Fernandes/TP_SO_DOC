#include "../include/funciones_storage.h"

// --- Variables Globales del FS ---
int g_bitmap_fd;
t_bitarray *g_bitmap;
t_dictionary *g_hash_index;
pthread_mutex_t g_mutex_bitmap;
pthread_mutex_t g_mutex_hash_index;
pthread_mutex_t g_mutex_fs;
int g_worker_count = 0;
pthread_mutex_t g_mutex_worker_count;
int fs_size;
int block_size;
void *g_bitmap_data;

static void cargar_filesystem();

/**
 * @brief Calcula el hash de un buffer de datos y lo agrega al índice de hashes
 * (en memoria y en disco) si no existe ya.
 * Esta función ASUME que cualquier hash ANTIGUO para ese nro_bloque_fisico
 * ya ha sido invalidado.
 */
static void actualizar_indice_hash(void *buffer_datos, int nro_bloque_fisico)
{
    // 1. Calcular el hash del buffer
    char *hash_nuevo = crypto_md5(buffer_datos, block_size);

    // 2. Bloquear el mutex del índice de hashes
    pthread_mutex_lock(&g_mutex_hash_index);

    // 3. Revisar si este hash ya existe (apuntando a otro bloque)
    if (!dictionary_has_key(g_hash_index, hash_nuevo))
    {
        // --- Contenido nuevo: agregar al índice de hashes ---
        int *nro_bloque_ptr = malloc(sizeof(int));
        *nro_bloque_ptr = nro_bloque_fisico;

        // Guardar copia del hash y el puntero al int en el diccionario
        dictionary_put(g_hash_index, strdup(hash_nuevo), nro_bloque_ptr);

        // 4. Persistir el cambio en el archivo 'blocks_hash_index.config'
        char *path_hash_index_file = string_from_format("%s/blocks_hash_index.config", punto_montaje);
        FILE *f_hash_index = fopen(path_hash_index_file, "a"); // Abrir en modo "append" (agregar al final)

        if (f_hash_index)
        {
            fprintf(f_hash_index, "%s=block%04d\n", hash_nuevo, nro_bloque_fisico);
            fclose(f_hash_index);
            log_debug(storage_logger, "Hash %s para bloque %d persistido en 'blocks_hash_index.config'.", hash_nuevo, nro_bloque_fisico);
        }
        else
        {
            log_error(storage_logger, "Error: No se pudo abrir %s para persistir hash.", path_hash_index_file);
        }
        free(path_hash_index_file);
    }
    else
    {
        log_debug(storage_logger, "Hash %s para bloque %d ya existe en el índice (apuntando a otro bloque). No se agrega.", hash_nuevo, nro_bloque_fisico);
    }

    // 5. Desbloquear y limpiar
    pthread_mutex_unlock(&g_mutex_hash_index);
    free(hash_nuevo);
}

static void crear_archivo_metadata(const char *path_tag, const char *tamanio, const char *estado, const char *blocks)
{
    char *path_metadata = string_from_format("%s/metadata.config", path_tag);

    // IMPORTANTE: Crear el archivo vacío primero (config_create NO lo crea)
    FILE *f = fopen(path_metadata, "w");
    if (f == NULL)
    {
        log_error(storage_logger, "Error creando archivo metadata en %s: %s", path_metadata, strerror(errno));
        free(path_metadata);
        return;
    }
    fclose(f);

    // Ahora sí, abrir con config_create (el archivo ya existe)
    t_config *config_metadata = config_create(path_metadata);
    if (config_metadata == NULL)
    {
        log_error(storage_logger, "Error abriendo config de metadata en %s", path_metadata);
        free(path_metadata);
        return;
    }

    config_set_value(config_metadata, "TAMAÑO", (char *)tamanio);
    config_set_value(config_metadata, "ESTADO", (char *)estado);
    config_set_value(config_metadata, "BLOCKS", (char *)blocks);

    if (config_save(config_metadata) == -1)
    {
        log_error(storage_logger, "Error guardando metadata en %s", path_metadata);
    }

    config_destroy(config_metadata);
    free(path_metadata);

    char *path_files_dir = string_from_format("%s/files/", punto_montaje);
    char *relative_path = strstr(path_tag, path_files_dir);
    if (relative_path != NULL)
    {
        relative_path += strlen(path_files_dir);
    }
    log_debug(storage_logger, "Metadata creada para: %s (Tamaño=%s, Estado=%s, Blocks=%s)", relative_path ? relative_path : path_tag, tamanio, estado, blocks);
    free(path_files_dir);
}

static char *reconstruir_string_de_bloques(char **array_bloques)
{
    char *bloques_str = string_new();
    string_append(&bloques_str, "[");

    if (array_bloques != NULL && array_bloques[0] != NULL)
    {
        for (int i = 0; array_bloques[i] != NULL; i++)
        {
            string_append(&bloques_str, array_bloques[i]);
            if (array_bloques[i + 1] != NULL)
            {
                string_append(&bloques_str, ",");
            }
        }
    }

    string_append(&bloques_str, "]");
    return bloques_str;
}

static void eliminar_hash_de_archivo(const char *hash_a_eliminar)
{
    char *path_hash_index_file = string_from_format("%s/blocks_hash_index.config", punto_montaje);
    char *path_hash_index_temp = string_from_format("%s/blocks_hash_index.tmp", punto_montaje);

    FILE *f_in = fopen(path_hash_index_file, "r");
    FILE *f_out = fopen(path_hash_index_temp, "w");

    if (!f_in || !f_out)
    {
        log_error(storage_logger, "Error abriendo archivos de hash para actualización.");
        if (f_in)
            fclose(f_in);
        if (f_out)
            fclose(f_out);
        free(path_hash_index_file);
        free(path_hash_index_temp);
        return;
    }

    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    char *line_start_ptr = NULL;

    while ((read = getline(&line, &len, f_in)) != -1)
    {
        line_start_ptr = line;
        // Comprobar si la línea comienza con el hash a eliminar
        if (strncmp(line_start_ptr, hash_a_eliminar, strlen(hash_a_eliminar)) == 0 && line_start_ptr[strlen(hash_a_eliminar)] == '=')
        {
            // No escribir esta línea en el archivo temporal
            continue;
        }
        fputs(line_start_ptr, f_out);
    }

    free(line);
    fclose(f_in);
    fclose(f_out);

    // Reemplazar el archivo original con el temporal
    remove(path_hash_index_file);
    rename(path_hash_index_temp, path_hash_index_file);

    free(path_hash_index_file);
    free(path_hash_index_temp);
}

static void formatear_filesystem(const char *punto_montaje, int fs_size, int block_size)
{
    log_debug(storage_logger, "[STORAGE] Iniciando formateo del File System");

    // 1. Limpiar y crear directorios base
    char *comando_limpieza = string_from_format("rm -rf %s/files %s/physical_blocks %s/bitmap.bin %s/blocks_hash_index.config",
                                                punto_montaje, punto_montaje, punto_montaje, punto_montaje);
    system(comando_limpieza);
    free(comando_limpieza);

    char *path_directorio_files = string_from_format("%s/files", punto_montaje);
    char *path_directorio_bloques = string_from_format("%s/physical_blocks", punto_montaje);
    mkdir(path_directorio_files, 0777);
    mkdir(path_directorio_bloques, 0777);

    // 1.5. Recrear superblock.config
    char *path_superbloque = string_from_format("%s/superblock.config", punto_montaje);
    FILE *f_super = fopen(path_superbloque, "w");
    fprintf(f_super, "FS_SIZE=%d\n", fs_size);
    fprintf(f_super, "BLOCK_SIZE=%d\n", block_size);
    fclose(f_super);
    free(path_superbloque);

    int cantidad_bloques = fs_size / block_size;

    // 2. Crear bitmap.bin
    char *path_bitmap = string_from_format("%s/bitmap.bin", punto_montaje);
    int tamanio_bitmap = (cantidad_bloques + 7) / 8; // redondeo correcto
    int fd_bitmap = open(path_bitmap, O_RDWR | O_CREAT | O_TRUNC, 0664);
    if (fd_bitmap == -1)
    {
        log_error(storage_logger, "Error creando bitmap: %s", strerror(errno));
        free(path_bitmap);
        free(path_directorio_files);
        free(path_directorio_bloques);
        return;
    }

    if (ftruncate(fd_bitmap, tamanio_bitmap) == -1)
    {
        log_error(storage_logger, "Error truncando bitmap: %s", strerror(errno));
        close(fd_bitmap);
        free(path_bitmap);
        free(path_directorio_files);
        free(path_directorio_bloques);
        return;
    }

    close(fd_bitmap);
    log_info(storage_logger, "Bitmap creado en %s (Tamaño: %d bytes)", basename(path_bitmap), tamanio_bitmap);
    free(path_bitmap);

    // 3. Crear blocks_hash_index.config
    char *path_indice_hashes = string_from_format("%s/blocks_hash_index.config", punto_montaje);
    FILE *filepath_file_indice_hashes = fopen(path_indice_hashes, "w");
    if (filepath_file_indice_hashes == NULL)
    {
        log_error(storage_logger, "Error creando índice de hashes: %s", strerror(errno));
        free(path_indice_hashes);
        free(path_directorio_files);
        free(path_directorio_bloques);
        return;
    }
    fclose(filepath_file_indice_hashes);
    log_info(storage_logger, "Índice de hashes creado en %s", basename(path_indice_hashes));
    free(path_indice_hashes);

    // 4. Crear archivos de bloques físicos
    void *buffer_ceros = calloc(1, block_size);
    if (buffer_ceros == NULL)
    {
        log_error(storage_logger, "Error de memoria al crear buffer de bloques");
        free(path_directorio_files);
        free(path_directorio_bloques);
        return;
    }

    for (int i = 0; i < cantidad_bloques; i++)
    {
        char *path_bloque = string_from_format("%s/block%04d.dat", path_directorio_bloques, i);
        FILE *f = fopen(path_bloque, "wb");
        if (f == NULL)
        {
            log_error(storage_logger, "Error creando bloque físico %d: %s", i, strerror(errno));
            free(path_bloque);
            continue;
        }
        fwrite(buffer_ceros, 1, block_size, f);
        fclose(f);
        free(path_bloque);
    }
    free(buffer_ceros);
    log_info(storage_logger, "%d bloques físicos creados en %s/", cantidad_bloques, basename(path_directorio_bloques));

    // 5. Crear initial_file
    // 5.1. Crear estructura de directorios
    char *path_initial_file = string_from_format("%s/initial_file", path_directorio_files);
    char *path_tag_base = string_from_format("%s/BASE", path_initial_file);
    char *path_bloques_logicos_base = string_from_format("%s/logical_blocks", path_tag_base);

    if (mkdir(path_initial_file, 0777) == -1 && errno != EEXIST)
    {
        log_error(storage_logger, "Error creando directorio initial_file: %s", strerror(errno));
    }

    if (mkdir(path_tag_base, 0777) == -1 && errno != EEXIST)
    {
        log_error(storage_logger, "Error creando directorio BASE: %s", strerror(errno));
    }

    if (mkdir(path_bloques_logicos_base, 0777) == -1 && errno != EEXIST)
    {
        log_error(storage_logger, "Error creando directorio logical_blocks: %s", strerror(errno));
    }

    // 5.2. Escribir '0's en el bloque físico 0
    char *path_bloque_cero = string_from_format("%s/block0000.dat", path_directorio_bloques);
    FILE *f_bloque_cero = fopen(path_bloque_cero, "r+b");
    if (f_bloque_cero == NULL)
    {
        log_error(storage_logger, "Error abriendo bloque físico 0: %s", strerror(errno));
        free(path_bloque_cero);
    }

    void *contenido_inicial = malloc(block_size);
    if (contenido_inicial == NULL)
    {
        log_error(storage_logger, "Error de memoria al crear contenido inicial");
        fclose(f_bloque_cero);
        free(path_bloque_cero);
    }

    memset(contenido_inicial, '0', block_size);
    size_t written = fwrite(contenido_inicial, 1, block_size, f_bloque_cero);
    fclose(f_bloque_cero);
    free(contenido_inicial);

    if (written != block_size)
    {
        log_error(storage_logger, "Error escribiendo en bloque físico 0 (escrito: %zu/%d)", written, block_size);
        free(path_bloque_cero);
    }

    log_debug(storage_logger, "Bloque físico 0 inicializado con '0's");

    // 5.3. Crear hard link del bloque lógico 0 al bloque físico 0
    char *path_bloque_logico_0 = string_from_format("%s/000000.dat", path_bloques_logicos_base);
    if (link(path_bloque_cero, path_bloque_logico_0) == -1)
    {
        log_error(storage_logger, "Error creando hard link de bloque lógico 0: %s", strerror(errno));
        free(path_bloque_logico_0);
        free(path_bloque_cero);
    }
    log_debug(storage_logger, "Hard link creado: .../%s -> .../%s", basename(path_bloque_logico_0), basename(path_bloque_cero));
    free(path_bloque_logico_0);
    free(path_bloque_cero);

    // 5.4. Crear metadata.config
    char *tamanio_bloque_str = string_itoa(block_size);
    crear_archivo_metadata(path_tag_base, tamanio_bloque_str, "COMMITED", "[0]");
    free(tamanio_bloque_str);

    log_info(storage_logger, "Archivo initial_file:BASE creado correctamente");

    free(path_bloques_logicos_base);
    free(path_tag_base);
    free(path_initial_file);
    free(path_directorio_files);
    free(path_directorio_bloques);

    // Cargar el bitmap recién creado y marcar el bloque 0 como ocupado.
    cargar_filesystem();
    pthread_mutex_lock(&g_mutex_bitmap);
    if (!bitarray_test_bit(g_bitmap, 0))
    {
        bitarray_set_bit(g_bitmap, 0);
        msync(g_bitmap_data, g_bitmap->size, MS_SYNC); // Persistir el cambio
        log_info(storage_logger, "Bloque 0 marcado como ocupado en bitmap para initial_file.");
    }
    pthread_mutex_unlock(&g_mutex_bitmap);

    log_info(storage_logger, "[STORAGE] Formateo del File System completado");
}

static void cargar_filesystem()
{
    // 1. Mapear 'bitmap.bin' a memoria y crear el t_bitarray.
    char *path_bitmap = string_from_format("%s/bitmap.bin", punto_montaje);
    g_bitmap_fd = open(path_bitmap, O_RDWR);

    struct stat info_bitmap;
    fstat(g_bitmap_fd, &info_bitmap);

    g_bitmap_data = mmap(NULL, info_bitmap.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, g_bitmap_fd, 0);

    if (info_bitmap.st_size == 0)
    {
        log_warning(storage_logger, "El archivo bitmap.bin está vacío. Esto puede causar errores.");
    }
    if (g_bitmap_data == MAP_FAILED)
    {
        log_error(storage_logger, "Error crítico: no se pudo mapear bitmap.bin a memoria");
        close(g_bitmap_fd);
        free(path_bitmap);
        exit(EXIT_FAILURE);
    }

    g_bitmap = bitarray_create_with_mode(g_bitmap_data, info_bitmap.st_size, LSB_FIRST);
    log_info(storage_logger, "Bitmap mapeado a memoria correctamente.");
    free(path_bitmap);

    // 2. Cargar 'blocks_hash_index.config' en el diccionario en memoria g_hash_index.
    char *path_indice_hashes = string_from_format("%s/blocks_hash_index.config", punto_montaje);
    FILE *f_hashes = fopen(path_indice_hashes, "r");
    if (f_hashes != NULL)
    {
        char *line = NULL;
        size_t len = 0;
        ssize_t read;

        while ((read = getline(&line, &len, f_hashes)) != -1)
        {
            // Eliminar el salto de línea
            if (line[read - 1] == '\n')
            {
                line[read - 1] = '\0';
            }

            char **parts = string_split(line, "=");
            if (parts[0] && parts[1])
            {
                char *hash_actual = parts[0];
                char *nombre_bloque = parts[1];

                int *numero_bloque = malloc(sizeof(int));
                sscanf(nombre_bloque, "block%d", numero_bloque);
                dictionary_put(g_hash_index, hash_actual, numero_bloque);
            }
            string_array_destroy(parts);
        }
        fclose(f_hashes);
        if (line)
            free(line);

        log_info(storage_logger, "Índice de hashes cargado en memoria: %d entradas.", dictionary_size(g_hash_index));
    }
    else
    {
        log_warning(storage_logger, "No se encontró o no se pudo leer 'blocks_hash_index.config'. Se asume vacío.");
    }
    free(path_indice_hashes);
}

// --- Inicialización/Destrucción del Filesystem ---
void inicializar_filesystem(const char *punto_montaje_config, const char *fresh_start)
{
    // Inicializar los mutex PRIMERO
    pthread_mutex_init(&g_mutex_bitmap, NULL);
    pthread_mutex_init(&g_mutex_hash_index, NULL);
    pthread_mutex_init(&g_mutex_fs, NULL);
    pthread_mutex_init(&g_mutex_worker_count, NULL);

    g_hash_index = dictionary_create();

    // Bloquear el mutex del FS durante todo el proceso de inicialización/formateo
    pthread_mutex_lock(&g_mutex_fs);

    // Leer superbloque para obtener los tamaños del FS. Usamos el parámetro, no la global.

    bool es_fresh_start = string_equals_ignore_case((char *)fresh_start, "TRUE");

    if (es_fresh_start)
    {
        char *path_superbloque = string_from_format("%s/superblock.config", punto_montaje_config);
        t_config *config_superbloque = config_create(path_superbloque);
        if (config_superbloque == NULL)
        {
            log_error(storage_logger, "Error crítico: no se pudo leer superblock.config en %s para formatear.", path_superbloque);
            exit(EXIT_FAILURE);
        }
        fs_size = config_get_int_value(config_superbloque, "FS_SIZE");
        block_size = config_get_int_value(config_superbloque, "BLOCK_SIZE");
        log_info(storage_logger, "Superbloque leído para formateo - FS_SIZE: %d, BLOCK_SIZE: %d", fs_size, block_size);
        config_destroy(config_superbloque);
        free(path_superbloque);

        formatear_filesystem(punto_montaje_config, fs_size, block_size);
    }
    else
    {
        char *path_superbloque = string_from_format("%s/superblock.config", punto_montaje_config);
        t_config *config_superbloque = config_create(path_superbloque);
        fs_size = config_get_int_value(config_superbloque, "FS_SIZE");
        block_size = config_get_int_value(config_superbloque, "BLOCK_SIZE");
        log_info(storage_logger, "Superbloque leído - FS_SIZE: %d, BLOCK_SIZE: %d", fs_size, block_size);
        config_destroy(config_superbloque);
        free(path_superbloque);
        cargar_filesystem(); // Cargar las estructuras existentes
    }

    pthread_mutex_unlock(&g_mutex_fs); // Desbloquear el mutex del FS cuando la inicialización termina

    log_info(storage_logger, "Estructuras del File System inicializadas en memoria.");
}

void destruir_filesystem()
{
    // Destruir los mutex al FINAL
    pthread_mutex_destroy(&g_mutex_bitmap);
    pthread_mutex_destroy(&g_mutex_hash_index);
    pthread_mutex_destroy(&g_mutex_fs);
    pthread_mutex_destroy(&g_mutex_worker_count);

    // Liberar bitmap
    if (g_bitmap != NULL)
    {
        munmap(g_bitmap_data, g_bitmap->size);
        bitarray_destroy(g_bitmap);
        close(g_bitmap_fd);
    }

    // Liberar diccionario de hashes y sus elementos (los int*)
    dictionary_destroy_and_destroy_elements(g_hash_index, free);

    log_info(storage_logger, "Estructuras del File System liberadas.");
}

//========================== FUNCIONES DE STORAGE ======================

void parse_file_tag(const char *file_tag_str, char **file, char **tag)
{
    char *copia = strdup(file_tag_str);
    *file = strdup(strtok(copia, ":"));
    *tag = strdup(strtok(NULL, ":"));
    free(copia);
}

op_code create_file(const char *file_name, const char *tag_name, int query_id)
{
    // Proteger las modificaciones de la estructura del sistema de archivos
    pthread_mutex_lock(&g_mutex_fs);

    char *path_file = string_from_format("%s/files/%s", punto_montaje, file_name);
    char *path_tag = string_from_format("%s/%s", path_file, tag_name);
    op_code resultado;

    struct stat info_tag = {0};
    if (stat(path_tag, &info_tag) == 0) // Revisar si el tag ya existe
    {
        resultado = STORAGE_ERROR_FILE_PREEXISTENTE;
    }
    else
    {
        mkdir(path_file, 0777); // Crear dir del file (seguro si ya existe)
        mkdir(path_tag, 0777);  // Crear dir del tag
        char *path_bloques_logicos = string_from_format("%s/logical_blocks", path_tag);
        mkdir(path_bloques_logicos, 0777); // Crear dir logical_blocks

        crear_archivo_metadata(path_tag, "0", "WORK_IN_PROGRESS", "[]");
        resultado = STORAGE_OK;

        free(path_bloques_logicos);
    }

    pthread_mutex_unlock(&g_mutex_fs); // Desbloquear después de las modificaciones

    free(path_file);
    free(path_tag);

    return resultado;
}

static int contar_referencias_a_bloque_fisico(int nro_bloque_fisico)
{
    // Contar hard links usando el campo st_nlink de la estructura stat
    char *path_bloque_fisico = string_from_format("%s/physical_blocks/block%04d.dat", punto_montaje, nro_bloque_fisico);
    struct stat info_bloque_fisico;

    if (stat(path_bloque_fisico, &info_bloque_fisico) == -1)
    {
        log_error(storage_logger, "No se pudo hacer stat sobre el bloque físico %d", nro_bloque_fisico);
        free(path_bloque_fisico);
        return 0;
    }

    free(path_bloque_fisico);

    // st_nlink cuenta todas las referencias, incluyendo el archivo original en /physical_blocks.
    return info_bloque_fisico.st_nlink - 1;
}

op_code truncate_file(const char *file_name, const char *tag_name, int nuevo_tamanio, int query_id)
{
    // Bloquear mutex del FS para acceder a metadata y a la estructura de directorios
    pthread_mutex_lock(&g_mutex_fs);

    char *path_tag = string_from_format("%s/files/%s/%s", punto_montaje, file_name, tag_name);
    char *path_metadata = string_from_format("%s/metadata.config", path_tag);
    op_code resultado = STORAGE_OK; // Asumir éxito inicialmente

    t_config *config_metadata = config_create(path_metadata);
    if (config_metadata == NULL)
    {
        resultado = STORAGE_ERROR_FILE_INEXISTENTE;
    }

    char *estado = config_get_string_value(config_metadata, "ESTADO");
    if (string_equals_ignore_case(estado, "COMMITED"))
    {
        resultado = STORAGE_ERROR_ESCRITURA_NO_PERMITIDA;
        log_debug(storage_logger, "##%d - truncate_file: escritura no permitida, ESTADO=COMMITED en %s:%s", query_id, file_name, tag_name);
    }

    int tamanio_actual = config_get_int_value(config_metadata, "TAMAÑO");
    int bloques_actuales = (tamanio_actual == 0) ? 0 : ((tamanio_actual - 1) / block_size + 1);
    int bloques_nuevos = (nuevo_tamanio == 0) ? 0 : ((nuevo_tamanio - 1) / block_size + 1);

    char **array_bloques = config_get_array_value(config_metadata, "BLOCKS");

    if (bloques_nuevos > bloques_actuales)
    {
        // --- AGRANDAR ARCHIVO ---
        // No se necesita bloquear el bitmap aquí, ya que solo enlazamos al bloque 0
        char *path_bloque_cero = string_from_format("%s/physical_blocks/block0000.dat", punto_montaje);
        for (int i = bloques_actuales; i < bloques_nuevos; i++)
        {
            char *path_bloque_logico = string_from_format("%s/logical_blocks/%06d.dat", path_tag, i);
            if (link(path_bloque_cero, path_bloque_logico) == -1)
            {
                log_error(storage_logger, "Error creando hard link: %s -> %s (%s)", path_bloque_cero, path_bloque_logico, strerror(errno));
                resultado = STORAGE_ERROR;
                free(path_bloque_logico);
                break;
            }
            // LOG MINIMO OBLIGATORIO
            log_info(storage_logger, "##%d - %s:%s Se agregó el hard link del bloque lógico %d al bloque físico 0", query_id, file_name, tag_name, i);
            string_array_push(&array_bloques, strdup("0"));
            free(path_bloque_logico);
        }
        free(path_bloque_cero);
    }
    else if (bloques_nuevos < bloques_actuales)
    {
        // --- ACHICAR ARCHIVO ---
        // Bloquear el bitmap específicamente para las potenciales operaciones de liberación
        pthread_mutex_lock(&g_mutex_bitmap);
        for (int i = bloques_actuales - 1; i >= bloques_nuevos; i--)
        {
            if (array_bloques[i] == NULL)
                continue;

            int nro_bloque_fisico = atoi(array_bloques[i]);
            char *path_bloque_logico = string_from_format("%s/logical_blocks/%06d.dat", path_tag, i);

            unlink(path_bloque_logico);
            free(path_bloque_logico);
            // LOG MINIMO OBLIGATORIO
            log_info(storage_logger, "##%d - %s:%s Se eliminó el hard link del bloque lógico %d al bloque físico %d", query_id, file_name, tag_name, i, nro_bloque_fisico);

            if (contar_referencias_a_bloque_fisico(nro_bloque_fisico) == 0)
            {
                bitarray_clean_bit(g_bitmap, nro_bloque_fisico);
                msync(g_bitmap_data, g_bitmap->size, MS_SYNC);
                // LOG MINIMO OBLIGATORIO
                log_info(storage_logger, "##%d - Bloque Físico Liberado - Número de Bloque: %d", query_id, nro_bloque_fisico);
            }
            free(string_array_pop(array_bloques));
        }
        pthread_mutex_unlock(&g_mutex_bitmap);
    }

    if (resultado == STORAGE_OK)
    {
        char *bloques_str = reconstruir_string_de_bloques(array_bloques);
        char *nuevo_tamanio_str = string_itoa(nuevo_tamanio);
        config_set_value(config_metadata, "TAMAÑO", nuevo_tamanio_str);
        config_set_value(config_metadata, "BLOCKS", bloques_str);
        config_save(config_metadata);
        free(nuevo_tamanio_str);
        free(bloques_str);
    }

    string_array_destroy(array_bloques);

    if (config_metadata)
        config_destroy(config_metadata);
    pthread_mutex_unlock(&g_mutex_fs); // Desbloquear mutex del FS
    free(path_tag);
    free(path_metadata);
    return resultado;
}

static int buscar_bloque_libre(int query_id)
{
    // Bloquear bitmap durante la búsqueda y asignación
    pthread_mutex_lock(&g_mutex_bitmap);
    int cantidad_bloques = fs_size / block_size;
    int bloque_encontrado = -1;

    log_trace(storage_logger, "%d - buscar_bloque_libre: comenzando búsqueda entre %d bloques (block_size=%d, fs_size=%d).", query_id, cantidad_bloques, block_size, fs_size);

    for (int i = 0; i < cantidad_bloques; i++)
    {
        if (!bitarray_test_bit(g_bitmap, i))
        {
            bitarray_set_bit(g_bitmap, i);
            msync(g_bitmap_data, g_bitmap->size, MS_SYNC);
            // LOG MINIMO OBLIGATORIO
            log_info(storage_logger, "##%d - Bloque Físico Reservado - Número de Bloque: %d", query_id, i);
            bloque_encontrado = i;
            break;
        }
    }
    pthread_mutex_unlock(&g_mutex_bitmap); // Desbloquear después de la búsqueda/asignación
    return bloque_encontrado;
}

static int validar_acceso_a_bloque(const char *file_name, const char *tag_name, int nro_bloque_logico, bool es_escritura, op_code *resultado, int query_id)
{
    char *path_tag = string_from_format("%s/files/%s/%s", punto_montaje, file_name, tag_name);
    char *path_metadata = string_from_format("%s/metadata.config", path_tag);
    int nro_bloque_fisico = -1;

    log_trace(storage_logger, "##%d - validar_acceso_a_bloque: %s:%s bloque_logico=%d es_escritura=%s", query_id, file_name, tag_name, nro_bloque_logico, es_escritura ? "TRUE" : "FALSE");

    t_config *config_metadata = config_create(path_metadata);
    if (config_metadata == NULL)
    {
        log_trace(storage_logger, "##%d - validar_acceso_a_bloque: metadata no existe para %s:%s", query_id, file_name, tag_name);

        *resultado = STORAGE_ERROR_FILE_INEXISTENTE;
    }
    else
    {
        char *estado = config_get_string_value(config_metadata, "ESTADO");
        int tamanio_archivo = config_get_int_value(config_metadata, "TAMAÑO");
        int bloques_totales = (tamanio_archivo == 0) ? 0 : ((tamanio_archivo - 1) / block_size + 1);

        if (es_escritura && string_equals_ignore_case(estado, "COMMITED"))
        {
            log_trace(storage_logger, "##%d - validar_acceso_a_bloque: escritura denegada por ESTADO=COMMITED en %s:%s", query_id, file_name, tag_name);

            *resultado = STORAGE_ERROR_ESCRITURA_NO_PERMITIDA;
        }
        else if (nro_bloque_logico >= bloques_totales)
        {
            log_trace(storage_logger, "##%d - validar_acceso_a_bloque: acceso fuera de límite (bloque_logico=%d >= bloques_totales=%d) para %s:%s", query_id, nro_bloque_logico, bloques_totales, file_name, tag_name);

            *resultado = es_escritura ? STORAGE_ERROR_ESCRITURA_FUERA_LIMITE : STORAGE_ERROR_LECTURA_FUERA_LIMITE;
        }
        else
        {
            char **array_bloques = config_get_array_value(config_metadata, "BLOCKS");
            nro_bloque_fisico = atoi(array_bloques[nro_bloque_logico]);
            log_trace(storage_logger, "##%d - validar_acceso_a_bloque: %s:%s bloque_logico=%d -> bloque_fisico=%d", query_id, file_name, tag_name, nro_bloque_logico, nro_bloque_fisico);

            string_array_destroy(array_bloques);
            *resultado = STORAGE_OK;
        }
        config_destroy(config_metadata);
    }

    free(path_tag);
    free(path_metadata);
    return nro_bloque_fisico;
}

static void invalidar_entrada_hash(int nro_bloque_fisico)
{
    char *clave_a_eliminar = NULL;

    // Función para iterar sobre el diccionario y encontrar la clave por valor
    void encontrar_clave(char *key, void *value)
    {
        if (*(int *)value == nro_bloque_fisico)
        {
            clave_a_eliminar = strdup(key);
        }
    }

    pthread_mutex_lock(&g_mutex_hash_index);

    dictionary_iterator(g_hash_index, encontrar_clave);

    if (clave_a_eliminar != NULL)
    {
        // Eliminar de la estructura en memoria
        void *old_value = dictionary_remove(g_hash_index, clave_a_eliminar);
        free(old_value);
        // Eliminar del archivo
        eliminar_hash_de_archivo(clave_a_eliminar);
        log_debug(storage_logger, "Hash '%s' para bloque %d invalidado por escritura.", clave_a_eliminar, nro_bloque_fisico);
        free(clave_a_eliminar);
    }

    pthread_mutex_unlock(&g_mutex_hash_index);
}

op_code write_block(const char *file_name, const char *tag_name, int nro_bloque_logico, void *buffer, int query_id)
{
    op_code resultado;
    pthread_mutex_lock(&g_mutex_fs);

    log_trace(storage_logger, "##%d - write_block: inicio %s:%s bloque_logico=%d", query_id, file_name, tag_name, nro_bloque_logico);

    int nro_bloque_fisico_actual = validar_acceso_a_bloque(file_name, tag_name, nro_bloque_logico, true, &resultado, query_id);

    if (resultado == STORAGE_OK)
    {
        log_trace(storage_logger, "##%d - write_block: acceso validado, bloque_fisico_actual=%d", query_id, nro_bloque_fisico_actual);

        char *path_tag = string_from_format("%s/files/%s/%s", punto_montaje, file_name, tag_name);
        char *path_metadata = string_from_format("%s/metadata.config", path_tag);
        char *path_bloque_logico = string_from_format("%s/logical_blocks/%06d.dat", path_tag, nro_bloque_logico);


        // Antes de escribir, invalidamos cualquier hash existente para este bloque físico
        log_trace(storage_logger, "##%d - write_block: invalidando hash previo del bloque fisico %d", query_id, nro_bloque_fisico_actual);
        invalidar_entrada_hash(nro_bloque_fisico_actual);
        log_trace(storage_logger, "##%d - write_block: hash invalidado (si existía) para bloque %d", query_id, nro_bloque_fisico_actual);

        // Chequeo de Copy-on-Write (CoW)
        if (contar_referencias_a_bloque_fisico(nro_bloque_fisico_actual) > 0)
        {
            log_trace(storage_logger, "##%d - write_block: CoW requerido (referencias > 0) para bloque fisico %d", query_id, nro_bloque_fisico_actual);

            int nro_bloque_fisico_nuevo = buscar_bloque_libre(query_id);
            if (nro_bloque_fisico_nuevo == -1)
            {
                log_trace(storage_logger, "##%d - write_block: realizando CoW -> nuevo bloque fisico %d", query_id, nro_bloque_fisico_nuevo);

                resultado = STORAGE_ERROR_ESPACIO_INSUFICIENTE;
            }
            else
            {
                // 1. Escribir el buffer en el nuevo bloque físico
                char *path_bloque_fisico_nuevo = string_from_format("%s/physical_blocks/block%04d.dat", punto_montaje, nro_bloque_fisico_nuevo);
                FILE *f_nuevo = fopen(path_bloque_fisico_nuevo, "wb");
                if (!f_nuevo || fwrite(buffer, 1, block_size, f_nuevo) != block_size)
                {
                    log_error(storage_logger, "Error de CoW: No se pudo escribir en el nuevo bloque físico %d", nro_bloque_fisico_nuevo);
                    if (f_nuevo)
                        fclose(f_nuevo);
                    resultado = STORAGE_ERROR;
                    // Liberar el bloque que no pudimos usar
                    pthread_mutex_lock(&g_mutex_bitmap);
                    bitarray_clean_bit(g_bitmap, nro_bloque_fisico_nuevo);
                    msync(g_bitmap_data, g_bitmap->size, MS_SYNC);
                    pthread_mutex_unlock(&g_mutex_bitmap);
                    log_trace(storage_logger, "##%d - write_block: CoW falló, bloque %d liberado", query_id, nro_bloque_fisico_nuevo);

                }
                else
                {
                    fclose(f_nuevo);
                    log_trace(storage_logger, "##%d - write_block: buffer escrito en nuevo bloque fisico %d", query_id, nro_bloque_fisico_nuevo);

                    // 2. Actualizar el hard link del bloque lógico para que apunte al nuevo bloque físico
                    unlink(path_bloque_logico);
                    if (link(path_bloque_fisico_nuevo, path_bloque_logico) == -1)
                    {
                        // LOG MINIMO OBLIGATORIO (implícito, se loguea el borrado)
                        log_info(storage_logger, "##%d - %s:%s Se eliminó el hard link del bloque lógico %d al bloque físico %d", query_id, file_name, tag_name, nro_bloque_logico, nro_bloque_fisico_actual);
                        log_error(storage_logger, "Error de CoW: No se pudo relinkear el bloque lógico %d al nuevo bloque físico %d", nro_bloque_logico, nro_bloque_fisico_nuevo);
                        resultado = STORAGE_ERROR;
                    }
                    else
                    {
                        // LOG MINIMO OBLIGATORIO
                        log_info(storage_logger, "##%d - %s:%s Se agregó el hard link del bloque lógico %d al bloque físico %d", query_id, file_name, tag_name, nro_bloque_logico, nro_bloque_fisico_nuevo);
                        log_trace(storage_logger, "##%d - write_block: hard link actualizado para %s:%s bloque_logico=%d -> bloque_fisico=%d", query_id, file_name, tag_name, nro_bloque_logico, nro_bloque_fisico_nuevo);

                        // 3. Actualizar la metadata del archivo
                        t_config *config_metadata = config_create(path_metadata);
                        char **array_bloques = config_get_array_value(config_metadata, "BLOCKS");

                        free(array_bloques[nro_bloque_logico]);
                        array_bloques[nro_bloque_logico] = string_itoa(nro_bloque_fisico_nuevo);

                        char *bloques_str = reconstruir_string_de_bloques(array_bloques);
                        config_set_value(config_metadata, "BLOCKS", bloques_str);
                        config_save(config_metadata);

                        config_destroy(config_metadata);
                        string_array_destroy(array_bloques);
                        free(bloques_str);
                    }

                    // --- Actualizar el hash para el NUEVO bloque ---
                    actualizar_indice_hash(buffer, nro_bloque_fisico_nuevo);
                    log_trace(storage_logger, "##%d - write_block: indice hash actualizado para bloque %d", query_id, nro_bloque_fisico_nuevo);

                }
                free(path_bloque_fisico_nuevo);
            }
        }
        else
        { // Escritura in-place
            log_trace(storage_logger, "##%d - write_block: escritura in-place en bloque_fisico=%d", query_id, nro_bloque_fisico_actual);

            char *path_bloque_fisico_actual_str = string_from_format("%s/physical_blocks/block%04d.dat", punto_montaje, nro_bloque_fisico_actual);
            FILE *f_actual = fopen(path_bloque_fisico_actual_str, "wb");
            if (!f_actual || fwrite(buffer, 1, block_size, f_actual) != block_size)
            {
                log_error(storage_logger, "Error de escritura in-place en bloque físico %d", nro_bloque_fisico_actual);
                if (f_actual)
                    fclose(f_actual);
                resultado = STORAGE_ERROR;
            }
            else
            {
                fclose(f_actual);
                log_debug(storage_logger, "Escritura in-place en bloque físico %d para '%s:%s'.", nro_bloque_fisico_actual, file_name, tag_name);
                actualizar_indice_hash(buffer, nro_bloque_fisico_actual);
                log_trace(storage_logger, "##%d - write_block: indice hash actualizado para bloque %d (in-place)", query_id, nro_bloque_fisico_actual);

            }
            free(path_bloque_fisico_actual_str);
        }
        free(path_tag);
        free(path_metadata);
        free(path_bloque_logico);
    }
    pthread_mutex_unlock(&g_mutex_fs);
    return resultado;
}

op_code read_block(const char *file_name, const char *tag_name, int nro_bloque_logico, void **buffer_lectura, int query_id)
{

    op_code resultado;
    // Bloquear mutex del FS para asegurar que la metadata no cambie durante la lectura
    pthread_mutex_lock(&g_mutex_fs);

    log_trace(storage_logger, "##%d - read_block: inicio %s:%s bloque_logico=%d", query_id, file_name, tag_name, nro_bloque_logico);

    int nro_bloque_fisico = validar_acceso_a_bloque(file_name, tag_name, nro_bloque_logico, false, &resultado, query_id);

    if (resultado == STORAGE_OK)
    {
        log_trace(storage_logger, "##%d - read_block: bloque_fisico=%d", query_id, nro_bloque_fisico);

        char *path_bloque_fisico = string_from_format("%s/physical_blocks/block%04d.dat", punto_montaje, nro_bloque_fisico);
        FILE *f = fopen(path_bloque_fisico, "rb");
        if (f)
        {
            *buffer_lectura = malloc(block_size);
            fread(*buffer_lectura, 1, block_size, f);
            fclose(f);
            resultado = STORAGE_OK;
            log_trace(storage_logger, "##%d - read_block: lectura completada %s:%s bloque_logico=%d -> bloque_fisico=%d", query_id, file_name, tag_name, nro_bloque_logico, nro_bloque_fisico);

        }
        else
        {
            resultado = STORAGE_ERROR;
            log_error(storage_logger, "##%d - read_block: error abriendo bloque fisico %d (%s:%s)", query_id, nro_bloque_fisico, file_name, tag_name);

        }
        free(path_bloque_fisico);
    }
    else {
        log_trace(storage_logger, "##%d - read_block: validacion de acceso fallo con codigo %d para %s:%s bloque_logico=%d", query_id, resultado, file_name, tag_name, nro_bloque_logico);
    }
    pthread_mutex_unlock(&g_mutex_fs); // Desbloquear mutex del FS
    return resultado;
}

op_code delete_tag(const char *file_name, const char *tag_name, int query_id)
{
    if (string_equals_ignore_case((char *)file_name, "initial_file") && string_equals_ignore_case((char *)tag_name, "BASE"))
    {
        log_error(storage_logger, "##%d - Intento de borrar archivo protegido initial_file:BASE", query_id);
        return STORAGE_ERROR_DELETE_NO_PERMITIDO;
    }

    log_trace(storage_logger, "##%d - delete_tag: inicio %s:%s", query_id, file_name, tag_name);

    // Bloquear FS para acceso a metadata y borrado de directorios
    // Bloquear Bitmap para liberar bloques
    pthread_mutex_lock(&g_mutex_fs);
    pthread_mutex_lock(&g_mutex_bitmap);

    char *path_tag = string_from_format("%s/files/%s/%s", punto_montaje, file_name, tag_name);
    char *path_metadata = string_from_format("%s/metadata.config", path_tag);
    op_code resultado = STORAGE_OK;

    t_config *config_metadata = config_create(path_metadata);
    if (config_metadata == NULL)
    {
        resultado = STORAGE_ERROR_TAG_INEXISTENTE;
    }
    else
    {
        char **array_bloques = config_get_array_value(config_metadata, "BLOCKS");

        // Liberar bloques físicos que quedaron sin referencias
        for (int i = 0; array_bloques[i] != NULL; i++)
        {
            int nro_bloque_fisico = atoi(array_bloques[i]);
            if (contar_referencias_a_bloque_fisico(nro_bloque_fisico) == 0)
            {
                bitarray_clean_bit(g_bitmap, nro_bloque_fisico);
                
                // LOG MINIMO OBLIGATORIO
                log_info(storage_logger, "##%d - Bloque Físico Liberado - Número de Bloque: %d", query_id, nro_bloque_fisico);
                log_trace(storage_logger, "##%d - delete_tag: bloque_fisico %d liberado", query_id, nro_bloque_fisico);

                log_debug(storage_logger, "Bloque físico %d liberado al eliminar tag.", nro_bloque_fisico);
            }
        }

        char *comando_borrado = string_from_format("rm -rf %s", path_tag);
        system(comando_borrado);
        free(comando_borrado);
        string_array_destroy(array_bloques);
        config_destroy(config_metadata);
    }

    pthread_mutex_unlock(&g_mutex_bitmap); // Desbloquear bitmap
    pthread_mutex_unlock(&g_mutex_fs);     // Desbloquear FS
    free(path_tag);
    free(path_metadata);
    return resultado;
}

op_code tag_file(const char *origen_file, const char *origen_tag, const char *destino_file, const char *destino_tag, int query_id)
{
    // Bloquear mutex del FS para proteger la estructura de directorios y metadata
    pthread_mutex_lock(&g_mutex_fs);

    // --- 1. Definir todas las rutas ---
    op_code resultado = STORAGE_OK;
    char *path_origen = string_from_format("%s/files/%s/%s", punto_montaje, origen_file, origen_tag);
    char *path_metadata_origen = string_from_format("%s/metadata.config", path_origen);

    char *path_destino_file = string_from_format("%s/files/%s", punto_montaje, destino_file);
    char *path_destino = string_from_format("%s/files/%s/%s", punto_montaje, destino_file, destino_tag);
    char *path_metadata_destino = string_from_format("%s/metadata.config", path_destino);
    char *path_logical_destino = string_from_format("%s/logical_blocks", path_destino);

    t_config *metadata_origen = NULL;
    char **array_bloques_origen = NULL;
    struct stat st_origen = {0};
    struct stat st_destino = {0};

    // --- 2. Validaciones Previas ---
    if (stat(path_origen, &st_origen) == -1)
    {
        log_error(storage_logger, "Error TAG: El origen '%s:%s' no existe.", origen_file, origen_tag);
        resultado = STORAGE_ERROR_TAG_INEXISTENTE;
    }
    if (stat(path_destino, &st_destino) == 0)
    {
        log_error(storage_logger, "Error TAG: El destino '%s:%s' ya existe.", destino_file, destino_tag);
        resultado = STORAGE_ERROR_FILE_PREEXISTENTE;
    }
    metadata_origen = config_create(path_metadata_origen);
    if (metadata_origen == NULL)
    {
        log_error(storage_logger, "Error TAG: No se pudo leer el metadata origen '%s'", path_metadata_origen);
        resultado = STORAGE_ERROR;
    }

    // --- 3. Crear Directorios Destino ---
    mkdir(path_destino_file, 0777); // Crear dir del file destino (seguro si existe)
    if (mkdir(path_destino, 0777) == -1)
    {
        log_error(storage_logger, "Error TAG: No se pudo crear el directorio destino '%s' (%s)", path_destino, strerror(errno));
        resultado = STORAGE_ERROR;
    }
    if (mkdir(path_logical_destino, 0777) == -1)
    {
        log_error(storage_logger, "Error TAG: No se pudo crear el directorio logical_blocks destino '%s' (%s)", path_logical_destino, strerror(errno));
        resultado = STORAGE_ERROR;
    }

    // --- 4. Copiar Metadata (Versión C, sin system("cp")) ---
    char *tamano_str = config_get_string_value(metadata_origen, "TAMAÑO");
    char *bloques_str = config_get_string_value(metadata_origen, "BLOCKS");

    // Usamos tu helper para crear el nuevo metadata, pero con estado WORK_IN_PROGRESS
    crear_archivo_metadata(path_destino, tamano_str, "WORK_IN_PROGRESS", bloques_str);
    log_debug(storage_logger, "Metadata copiada a '%s' y marcada como WORK_IN_PROGRESS.", path_destino);

    // --- 5. Recrear Hard Links (Esta lógica ya la tenías bien) ---
    array_bloques_origen = config_get_array_value(metadata_origen, "BLOCKS");

    for (int i = 0; array_bloques_origen[i] != NULL; i++)
    {
        int nro_bloque_fisico = atoi(array_bloques_origen[i]);
        char *path_bloque_fisico = string_from_format("%s/physical_blocks/block%04d.dat", punto_montaje, nro_bloque_fisico);
        char *path_bloque_logico_destino = string_from_format("%s/%06d.dat", path_logical_destino, i);

        // Crear el hard link desde el bloque físico original al nuevo bloque lógico
        if (link(path_bloque_fisico, path_bloque_logico_destino) == -1)
        {
            log_error(storage_logger, "Error TAG: No se pudo crear hard link para bloque %d -> %s (%s)", i, path_bloque_logico_destino, strerror(errno));
            resultado = STORAGE_ERROR;
            free(path_bloque_fisico);
            free(path_bloque_logico_destino);
            break; // Salir del bucle
        }

        // LOG MINIMO OBLIGATORIO
        log_info(storage_logger, "##%d - %s:%s Se agregó el hard link del bloque lógico %d al bloque físico %d", query_id, destino_file, destino_tag, i, nro_bloque_fisico);
        free(path_bloque_fisico);
        free(path_bloque_logico_destino);
    }

    // LOG MINIMO OBLIGATORIO (si todo salió bien)
    if (resultado == STORAGE_OK)
    {
        log_info(storage_logger, "## %d - Tag creado %s:%s", query_id, destino_file, destino_tag);
    }

    // --- 6. Liberar todos los recursos ---
    if (metadata_origen)
        config_destroy(metadata_origen);
    if (array_bloques_origen)
        string_array_destroy(array_bloques_origen);

    free(path_origen);
    free(path_metadata_origen);
    free(path_destino_file);
    free(path_destino);
    free(path_metadata_destino);
    free(path_logical_destino);

    pthread_mutex_unlock(&g_mutex_fs); // Desbloquear mutex del FS
    return resultado;
}

op_code commit_tag(const char *file_name, const char *tag_name, int query_id)
{
    // Bloquear mutex del FS para leer/escribir metadata y potencialmente modificar links
    pthread_mutex_lock(&g_mutex_fs);

    char *path_tag = string_from_format("%s/files/%s/%s", punto_montaje, file_name, tag_name);
    char *path_metadata = string_from_format("%s/metadata.config", path_tag);
    char *path_hash_index_file = string_from_format("%s/blocks_hash_index.config", punto_montaje);
    op_code resultado = STORAGE_OK;
    t_config *config_metadata = NULL;
    char **array_bloques = NULL;
    void *buffer_lectura = NULL;
    bool metadata_modificado = false;

    config_metadata = config_create(path_metadata);
    if (config_metadata == NULL)
    {
        resultado = STORAGE_ERROR_TAG_INEXISTENTE;
        // Limpiar y desbloquear antes de salir
        pthread_mutex_unlock(&g_mutex_fs);
        free(path_tag);
        free(path_metadata);
        free(path_hash_index_file);
        return resultado;
    }

    char *estado = config_get_string_value(config_metadata, "ESTADO");
    if (string_equals_ignore_case(estado, "COMMITED"))
    {
        // Ya está confirmado, no hacer nada.
        config_destroy(config_metadata);
        pthread_mutex_unlock(&g_mutex_fs);
        free(path_tag);
        free(path_metadata);
        free(path_hash_index_file);
        return STORAGE_OK;
    }

    array_bloques = config_get_array_value(config_metadata, "BLOCKS");
    buffer_lectura = malloc(block_size);
    if (buffer_lectura == NULL)
    {
        log_error(storage_logger, "Error COMMIT: malloc falló para buffer de lectura.");
        resultado = STORAGE_ERROR;
    }

    for (int i = 0; array_bloques && array_bloques[i] != NULL; i++)
    {
        int nro_bloque_actual = atoi(array_bloques[i]);
        char *path_bloque_actual = string_from_format("%s/physical_blocks/block%04d.dat", punto_montaje, nro_bloque_actual);
        char *hash_actual = NULL;
        int *nro_bloque_existente_ptr = NULL;

        FILE *f = fopen(path_bloque_actual, "rb");
        if (!f || fread(buffer_lectura, 1, block_size, f) != block_size)
        {
            log_error(storage_logger, "Error COMMIT: leyendo bloque físico %d", nro_bloque_actual);
            if (f) fclose(f);
            free(path_bloque_actual);
            resultado = STORAGE_ERROR;
            continue;
        }
        fclose(f);

        hash_actual = crypto_md5(buffer_lectura, block_size);

        // Bloquear índice de hashes mientras lo consultamos/modificamos
        pthread_mutex_lock(&g_mutex_hash_index);

        if (dictionary_has_key(g_hash_index, hash_actual))
        {
            nro_bloque_existente_ptr = dictionary_get(g_hash_index, hash_actual);
            int nro_bloque_existente = *nro_bloque_existente_ptr;
            if (nro_bloque_existente != nro_bloque_actual)
            {
                // Hay otro bloque con el mismo hash: deduplicar
                free(array_bloques[i]);
                array_bloques[i] = string_itoa(nro_bloque_existente);
                metadata_modificado = true;
                // Ajustes de contadores y bitarray si corresponde (si se liberó) se hacen más abajo
            }
        }
        else
        {
            // Nuevo hash: agregar al índice (en memoria y persistir)
            int *ptr = malloc(sizeof(int));
            *ptr = nro_bloque_actual;
            dictionary_put(g_hash_index, strdup(hash_actual), ptr);

            // Persistir en blocks_hash_index.config
            FILE *f_idx = fopen(path_hash_index_file, "a");
            if (f_idx)
            {
                fprintf(f_idx, "%s=block%04d\n", hash_actual, nro_bloque_actual);
                fclose(f_idx);
            }
        }

        pthread_mutex_unlock(&g_mutex_hash_index);

        free(hash_actual);
        free(path_bloque_actual);
    } // Fin del bucle for

    // Guardar cambios en metadata si hubo deduplicación o para cambiar estado
    if (metadata_modificado)
    {
        char *bloques_str = reconstruir_string_de_bloques(array_bloques);
        config_set_value(config_metadata, "BLOCKS", bloques_str);
        free(bloques_str);
    }
    config_set_value(config_metadata, "ESTADO", "COMMITED");
    config_save(config_metadata);

    // Liberar recursos
    if (buffer_lectura)
        free(buffer_lectura);
    if (array_bloques)
        string_array_destroy(array_bloques);
    if (config_metadata)
        config_destroy(config_metadata);
    free(path_tag);
    free(path_metadata);
    free(path_hash_index_file);

    pthread_mutex_unlock(&g_mutex_fs); // Desbloquear mutex del FS
    return resultado;
}

op_code fs_get_file_info(const char* file_name, const char* tag_name, int query_id, int* out_tamanio, char** out_estado)
{
    // Bloqueamos para una lectura segura del metadata
    pthread_mutex_lock(&g_mutex_fs);

    char* path_tag = string_from_format("%s/files/%s/%s", punto_montaje, file_name, tag_name);
    char* path_metadata = string_from_format("%s/metadata.config", path_tag);
    op_code resultado = STORAGE_OK;
    
    t_config* config_metadata = config_create(path_metadata);
    
    if (config_metadata == NULL) {
        log_warning(storage_logger, "##%d - GET_FILE_INFO: No se encontró el metadata para %s:%s.", query_id, file_name, tag_name);
        resultado = STORAGE_ERROR_TAG_INEXISTENTE;
    } else {
        *out_tamanio = config_get_int_value(config_metadata, "TAMAÑO");
        *out_estado = strdup(config_get_string_value(config_metadata, "ESTADO"));
        config_destroy(config_metadata);
        
        log_info(storage_logger, "##%d - GET_FILE_INFO: Enviando info de %s:%s (Tamaño: %d, Estado: %s)", query_id, file_name, tag_name, *out_tamanio, *out_estado);
    }

    pthread_mutex_unlock(&g_mutex_fs);
    free(path_tag);
    free(path_metadata);
    return resultado;
}