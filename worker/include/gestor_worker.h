#ifndef GESTOR_WORKER_H_
#define GESTOR_WORKER_H_

#include "../../utils/include/utils.h"

//==============================
//== ARCHIVO DE CONFIGURACION ==
//==============================
extern int fd_conexion_master;
extern int fd_conexion_storage;

extern char* ip_master;
extern char* puerto_master;
extern char* ip_storage;
extern char* puerto_storage;
extern int tam_memoria;
extern int retardo_memoria;
extern char* algoritmo_reemplazo;
extern char* path_query;
extern int tam_pagina;
extern int fs_size;

extern t_log_level log_level;
extern t_log* worker_logger;
extern t_config* worker_config;
extern char* worker_id;
extern char* archivo_config;

extern t_list* instrucciones;
extern int pc; 
extern int current_query_id;
extern int tamano_pagina;
extern int tam_pagina;
extern int fs_size;
extern bool desalojar_query_en_proceso;
extern bool query_cancelada;

extern sem_t sem_procesamiento;

//=================
//== ESTRUCTURAS ==
//=================
typedef enum {
    NINGUNA_INTERRUPCION,
    DESALOJO,
    CANCELACION
} t_tipo_interrupcion;

typedef struct {
    int query_id;
    int pc;
    t_tipo_interrupcion interrupcion; 
    t_list* instrucciones;
} t_query_context;


extern t_query_context* contexto;

extern t_list* instrucciones;
extern int pc;
extern int current_query_id;
//=========================
//== ENUM INSTRUCCIONES  ==
//=========================
typedef enum {
    CREATE, 
    TRUNCATE, 
    WRITE, 
    READ, 
    TAG, 
    COMMIT, 
    FLUSH, 
    DELETE, 
    END_OP, 
    INVALID
} t_opcode;

// ESTRUCTURAS PARA MEMORIA INTERNA
struct t_tabla_de_paginas;

typedef struct {
    int nro_marco;
    bool bit_presencia;
    bool bit_modificado;
    bool bit_uso;
    time_t timestamp_ultimo_uso;
} t_pagina;

typedef enum {
    WORK_IN_PROGRESS,
    COMMITED
} t_estado_file; // Estado del archivo en nuestra memoria

typedef struct {
    char* file_tag_id;
    t_pagina* paginas;
    int cant_paginas;
    t_estado_file estado; 
} t_tabla_de_paginas;

typedef struct {
    bool esta_libre; // true si el marco esta libre, false si esta ocupado
    struct t_tabla_de_paginas* tabla_dueña;
    int nro_pagina_dueña;
} t_marco;

extern void* espacio_memoria_principal;
extern t_marco* tabla_de_marcos;
extern t_dictionary* tabla_paginas_global;

#endif