#ifndef FUNCIONES_STORAGE_H_
#define FUNCIONES_STORAGE_H_

#include "gestor_storage.h"
#include <errno.h>
#include <libgen.h> 


void inicializar_filesystem(const char* punto_montaje, const char* fresh_start);
void destruir_filesystem();
void parse_file_tag(const char* file_tag_str, char** file, char** tag);

// ======================= OPERACIONES DEL FS =======================

op_code create_file(const char *file_name, const char *tag_name, int query_id);
op_code truncate_file(const char *file_name, const char *tag_name, int nuevo_tamanio, int query_id);
op_code write_block(const char *file_name, const char *tag_name, int nro_bloque_logico, void *buffer, int query_id);
op_code read_block(const char *file_name, const char *tag_name, int nro_bloque_logico, void **buffer_lectura, int query_id);
op_code delete_tag(const char* file_name, const char* tag_name, int query_id);
op_code tag_file(const char* origen_file, const char* origen_tag, const char* destino_file, const char* destino_tag, int query_id);
op_code commit_tag(const char* file_name, const char* tag_name, int query_id);
op_code fs_get_file_info(const char* file_name, const char* tag_name, int query_id, int* out_tamanio, char** out_estado);




#endif