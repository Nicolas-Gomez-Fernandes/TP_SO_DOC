#ifndef GESTOR_STORAGE_H_
#define GESTOR_STORAGE_H_

#include "../../utils/include/utils.h"

//==============================
//== ARCHIVO DE CONFIGURACION ==
//==============================
extern char* puerto_storage;
extern char* fresh_start;
extern char* punto_montaje; // Ruta raíz del FS
extern int retardo_operacion;
extern int retardo_acceso_bloque;

extern int fs_size;
extern int block_size;

extern t_log_level log_level;
extern t_log* storage_logger;
extern t_config* storage_config;
extern t_config* superblock_config;
extern int socket_servidor;

 //=================================
//==== ESTRUCTURAS DEL FILESYSTEM ====
//=================================

// --- Bitmap de bloques ---
extern void* g_bitmap_data;         // Puntero a la memoria mapeada del archivo bitmap.bin
extern t_bitarray* g_bitmap;        // Estructura de bitarray de las commons
extern pthread_mutex_t g_mutex_bitmap;

// --- Índice de Hashes ---
extern t_dictionary* g_hash_index;  // Diccionario en memoria para <hash, block_path>
extern pthread_mutex_t g_mutex_hash_index;

extern pthread_mutex_t g_mutex_worker_count;
extern int g_worker_count;

extern pthread_mutex_t g_mutex_fs;


#endif