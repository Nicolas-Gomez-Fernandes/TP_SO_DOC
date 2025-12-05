#include "../include/gestor_query.h"

// Definicion de variables globales 
int id_query_global = 0;
pthread_mutex_t mutex_id_query = PTHREAD_MUTEX_INITIALIZER;

t_lista* lista_querys = NULL;
t_lista* cola_ready = NULL; 
t_lista* cola_exec = NULL;

void inicializar_gestor_query(void) {
    id_query_global = 0; // Iniciar el ID global en 0
    pthread_mutex_init(&mutex_id_query, NULL);
    lista_querys = crear_lista(); // Crear la lista de queries
    cola_ready = crear_lista(); // Inicializar la cola de READY
    cola_exec = crear_lista();
}

void destruir_gestor_query(void) {
    if (cola_ready) {
        pthread_mutex_destroy(&cola_ready->mutex);
        list_destroy(cola_ready->lista); // Destruir la lista interna
        free(cola_ready);
        cola_ready = NULL;
    }
    if (cola_exec) {
        pthread_mutex_destroy(&cola_exec->mutex);
        list_destroy(cola_exec->lista); // Destruir la lista interna
        free(cola_exec);
        cola_exec = NULL;
    }
    destruir_lista_con_elementos(lista_querys, (void*)destruir_query);
    pthread_mutex_destroy(&mutex_id_query);
}

int obtener_nuevo_id() {
    pthread_mutex_lock(&mutex_id_query);
    int nuevo_id = id_query_global++;
    pthread_mutex_unlock(&mutex_id_query);
    return nuevo_id;
}

query_t* crear_query(char* path, int prioridad, int fd_query_control) {
    query_t* nueva_query = malloc(sizeof(query_t));
    nueva_query->id = obtener_nuevo_id();
    nueva_query->pc = 0;
    nueva_query->path = strdup(path);
    nueva_query->prioridad = prioridad;
    nueva_query->fd_query_control = fd_query_control;
    nueva_query->estado = READY;
    nueva_query->id_worker_asignado = -1;
    pthread_mutex_init(&nueva_query->mutex_interno, NULL);
    agregar_lista(lista_querys, nueva_query);
    agregar_lista(cola_ready, nueva_query);
    crear_hilo_aging_para_query(nueva_query);
    sem_post(&sem_replanificar);
    return nueva_query;
}

void destruir_query(query_t* query) {
    if (query) {
        free(query->path);
        pthread_mutex_destroy(&query->mutex_interno);
        free(query);
    }
}

void cambiar_estado_query(query_t* query, estado_query_t nuevo_estado) {
    pthread_mutex_lock(&query->mutex_interno);
    query->estado = nuevo_estado;
    pthread_mutex_unlock(&query->mutex_interno);
}

void asignar_worker_a_query(query_t* query, int id_worker) {
    if (query) {
        pthread_mutex_lock(&query->mutex_interno);
        query->id_worker_asignado = id_worker;
        query->estado = EXEC;
        agregar_lista(cola_exec, query);
        pthread_mutex_unlock(&query->mutex_interno);
    }
}

query_t* buscar_query_ready() {
    // leer de las config se la planificacion e FIFO
    if (strcmp(algoritmo_planificacion, "FIFO") == 0) {
        return pop_lista(cola_ready);
    } else {
        return remover_query_mayor_prioridad_ready();
    }
    return NULL;
}

// dado una prioridad, devuelve la query en exec con menor prioridad
query_t* get_query_menor_prioridad_exec(int prioridad) {
    pthread_mutex_lock(&cola_exec->mutex);
    query_t* menor = NULL;
    for (int i = 0; i < cola_exec->lista->elements_count; i++) {
        query_t* query = list_get(cola_exec->lista, i);
        if (query->prioridad > prioridad) {
            if (!menor || query->prioridad > menor->prioridad) {
                menor = query;
            }
        }
    }
    pthread_mutex_unlock(&cola_exec->mutex);
    return menor;
}

query_t* remover_query_mayor_prioridad_ready() {
    pthread_mutex_lock(&cola_ready->mutex);
    query_t* mayor = NULL;
    int idx_mayor = -1;
    for (int i = 0; i < cola_ready->lista->elements_count; i++) {
        query_t* query = list_get(cola_ready->lista, i);
        if (!mayor || query->prioridad < mayor->prioridad) { 
            mayor = query;
            idx_mayor = i;
        }   
    }
    if (idx_mayor != -1) {
        list_remove(cola_ready->lista, idx_mayor);
    }
    pthread_mutex_unlock(&cola_ready->mutex);
    return mayor;
}

query_t* get_query_mayor_prioridad_ready() {
    pthread_mutex_lock(&cola_ready->mutex);
    query_t* mayor = NULL;
    for (int i = 0; i < cola_ready->lista->elements_count; i++) {
        query_t* query = list_get(cola_ready->lista, i);
        if (!mayor || query->prioridad < mayor->prioridad) {  // Cambio: < en lugar de >
            mayor = query;
        }   
    }
    pthread_mutex_unlock(&cola_ready->mutex);
    return mayor;
}

// // buscar_query_por_id
query_t* buscar_y_remover_query_por_id(t_lista* querys, int id) {
    pthread_mutex_lock(&querys->mutex);
    for (int i = 0; i < querys->lista->elements_count; i++) {
        query_t* query = list_get(querys->lista, i);
        pthread_mutex_lock(&query->mutex_interno);
        if (query->id == id) {
            query_t* q = list_remove(querys->lista, i);
            pthread_mutex_unlock(&query->mutex_interno);
            pthread_mutex_unlock(&querys->mutex);
            return q;
        }
        pthread_mutex_unlock(&query->mutex_interno);
    }
    pthread_mutex_unlock(&querys->mutex);
    return NULL;
}

// Obtener nivel multiprocesamiento
int obtener_cantidad_querys() {
    pthread_mutex_lock(&lista_querys->mutex);
    int cantidad = list_size(lista_querys->lista);
    pthread_mutex_unlock(&lista_querys->mutex);
    return cantidad;
}

// ========================================
// ===== Funciones para Prioridades =====
// ========================================

// // Buscar la query READY con mayor prioridad
// query_t* buscar_query_ready_prioridad() {
//     pthread_mutex_lock(&cola_ready->mutex);
//     query_t* mejor = NULL;
//     int idx_mejor = -1;
//     for (int i = 0; i < cola_ready->lista->elements_count; i++) {
//         query_t* query = list_get(cola_ready->lista, i);
//         if (!mejor || query->prioridad < mejor->prioridad) {
//             mejor = query;
//             idx_mejor = i;
//         }
//     }
//     query_t* resultado = NULL;
//     if (idx_mejor != -1) {
//         resultado = list_remove(cola_ready->lista, idx_mejor);
//     }
//     pthread_mutex_unlock(&cola_ready->mutex);
//     return resultado;
// }

// // Buscar la query en ejecución con menor prioridad
// query_t* buscar_query_exec_menor_prioridad() {
//     pthread_mutex_lock(&cola_exec->mutex);
//     query_t* menor = NULL;
//     int idx_menor = -1;
//     for (int i = 0; i < cola_exec->lista->elements_count; i++) {
//         query_t* query = list_get(cola_exec->lista, i);
//         if (!menor || query->prioridad < menor->prioridad) {
//             menor = query;
//             idx_menor = i;
//         }
//     }
//     query_t* resultado = NULL;
//     if (idx_menor != -1) {
//         resultado = list_remove(cola_exec->lista, idx_menor);
//     }
//     pthread_mutex_unlock(&cola_exec->mutex);
//     return resultado;
// }