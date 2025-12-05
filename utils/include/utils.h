#ifndef UTILS_H_
#define UTILS_H_

#include <fcntl.h>
#include <dirent.h>
#include <commons/string.h>
#include <commons/crypto.h>
#include <commons/bitarray.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <readline/readline.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/collections/queue.h> 
#include <commons/collections/list.h>
#include <math.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <commons/collections/dictionary.h>

//=================
//==== OP CODE ====
//=================
typedef enum
{
    HANDSHAKE,
    HANDSHAKE_QUERY_CONTROL,
    HANDSHAKE_MASTER,
    HANDSHAKE_WORKER,
    HANDSHAKE_STORAGE,
    HANDSHAKE_OK,
    HANDSHAKE_ERROR,


    // Master - Query Control
    WORKER_DOWN,    // Notifica que el worker se desconecto
    END,                 // Worker da por finalizada la Query
    
    // Master - Worker
    ASIGNACION_QUERY,   // Master notifica al Worker la asignacion de una query
    QUERY_DOWN,         // Notifica que Query Control se desconecto
    DESALOJO_PC,        // respuesta de Worker al desalojar exitosamene una Query Control

    //op_code WORKER
    ASIGNAR_QUERY, 
    DESALOJAR_QUERY,
    QUERY_ERROR,
    QUERY_FINALIZADA,

    //op_code solicitudes WORKER --> STORAGE
    CREATE_FILE,
    TRUNCATE_FILE,
    WRITE_BLOCK_STORAGE, 
    READ_BLOCK_STORAGE,  
    TAG_FILE,            
    COMMIT_TAG,          
    DELETE_TAG,

    // --- NUEVO OP_CODE ---
    GET_FILE_INFO, // Worker pide info de un File:Tag

    //op_code respuesta STORAGE --> WORKER
    STORAGE_OK,
    STORAGE_ERROR,
    STORAGE_ERROR_FILE_INEXISTENTE,
    STORAGE_ERROR_TAG_INEXISTENTE,
    STORAGE_ERROR_ESPACIO_INSUFICIENTE,
    STORAGE_ERROR_ESCRITURA_NO_PERMITIDA,
    STORAGE_ERROR_LECTURA_FUERA_LIMITE,
    STORAGE_ERROR_ESCRITURA_FUERA_LIMITE,
    STORAGE_ERROR_FILE_PREEXISTENTE,
    STORAGE_ERROR_DELETE_NO_PERMITIDO,


    //op_code WORKER  --> MASTER 
    QUERY_READ_OK,  
}op_code;

//===========================
//==== HEADERS FUNCIONES ====
//===========================
t_log_level parse_log_level(const char* log_level_str);
int crear_conexion(char *ip, char *puerto);
int iniciar_servidor(char *puerto, t_log *un_log, char *msj_server);
int esperar_cliente(int socket_servidor, t_log *un_log, char *mensaje);
int recibir_operacion(int socket_cliente);
void enviar_operacion(int socket_fd, int cod_op);
int recibir_entero(int socket_fd, t_log* logger);
void enviar_entero(int socket_fd, t_log* logger, int valor);
void liberar_conexion(int socket_cliente);
char* recibir_string(int socket_fd, t_log* logger);
void enviar_string(int socket_fd, t_log* logger, char* string);
void protocolo_handshake_servidor(int socket);
void* recibir_buffer(int socket_fd, t_log* logger, int size);
void enviar_buffer(int socket_fd, t_log* logger, void* buffer, int size);

#endif