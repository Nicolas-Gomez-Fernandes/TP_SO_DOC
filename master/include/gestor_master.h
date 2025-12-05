#ifndef GESTOR_MASTER_H_
#define GESTOR_MASTER_H_

#include "../../utils/include/utils.h"
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <commons/collections/list.h>

//==============================
//== ARCHIVO DE CONFIGURACION ==
//==============================
extern char* puerto_escucha;
extern char* algoritmo_planificacion;
extern int tiempo_again;

extern t_log_level log_level;
extern t_log* master_logger;
extern t_config* master_config;

extern int fd_escucha;
extern int fd_cliente_query_control;
extern int fd_cliente_worker;

//==============================
// ===== Lista con mutex =======
//==============================

typedef struct {
    t_list* lista;
    pthread_mutex_t mutex;
} t_lista;

//==============================
// == Query Control ==
//==============================
typedef enum {
    READY,
    EXEC,
    EXIT
} estado_query_t;

typedef struct {
    int id;
    char* path;
    int prioridad;
    int pc;
    int fd_query_control;
    estado_query_t estado;
    int id_worker_asignado;
    pthread_mutex_t mutex_interno;
    // Campos para aging individual
    pthread_t hilo_aging;
    bool tiene_aging_activo;
} query_t;

extern t_lista* lista_querys;
extern int id_query_global;
extern pthread_mutex_t mutex_id_query;

// ===================
// == Worker ==
// ===================
typedef enum {
    WORKER_LIBRE,
    WORKER_OCUPADO,
    WORKER_DESCONECTADO
} estado_worker_t;

typedef struct {
    int id;                     // ID único del worker
    int fd;                     // File descriptor de la conexión
    estado_worker_t estado;     // Estado actual
    int query_asignada;         // ID de la query que está ejecutando (-1 si está libre)
    pthread_mutex_t mutex;      // Mutex para proteger la estructura
} worker_t;

extern t_lista* lista_workers;

//==============================
// == Ejecución ==
//==============================
extern sem_t sem_replanificar;
extern t_lista* cola_ready;
extern t_lista* cola_exec;

typedef struct {
    query_t* query;
    worker_t* worker;
} datos_ejecucion_t;

#endif