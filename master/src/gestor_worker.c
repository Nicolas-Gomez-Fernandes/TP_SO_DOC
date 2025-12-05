#include "../include/gestor_worker.h"


// Lista global de workers y su mutex
t_lista* lista_workers = NULL;

void inicializar_gestor_worker() {
    lista_workers = crear_lista();
}

void destruir_gestor_worker() {
    destruir_lista_con_elementos(lista_workers, (void*)destruir_worker);
}

worker_t* crear_worker(int id, int fd) {
    worker_t* worker = malloc(sizeof(worker_t));
    worker->id = id;
    worker->fd = fd;
    worker->estado = WORKER_LIBRE;
    worker->query_asignada = -1;
    pthread_mutex_init(&worker->mutex, NULL);
    agregar_worker(worker);
    sem_post(&sem_replanificar);
    // Log Obligatorio de Conexión de Worker
    log_info(master_logger, "## Se conecta el Worker %d Cantidad total de Workers: %d", id, obtener_cantidad_workers());
    return worker;
}

void destruir_worker(worker_t* w) {
    if(w) {
        pthread_mutex_destroy(&w->mutex);
        free(w);
    }
}

void agregar_worker(worker_t* worker) {
    pthread_mutex_lock(&lista_workers->mutex);
    list_add(lista_workers->lista, worker);
    pthread_mutex_unlock(&lista_workers->mutex);
}

worker_t* buscar_worker_por_id(int id) {
    pthread_mutex_lock(&lista_workers->mutex);
    worker_t* resultado = NULL;
    for (int i = 0; i < list_size(lista_workers->lista); i++) {
        worker_t* w = list_get(lista_workers->lista, i);
        if (w->id == id) {
            resultado = w;
            break;
        }
    }
    pthread_mutex_unlock(&lista_workers->mutex);
    return resultado;
}

worker_t* buscar_worker_libre() {
    pthread_mutex_lock(&lista_workers->mutex);
    worker_t* resultado = NULL;
    for (int i = 0; i < list_size(lista_workers->lista); i++) {
        worker_t* w = list_get(lista_workers->lista, i);
        pthread_mutex_lock(&w->mutex);
        if (w->estado == WORKER_LIBRE) {
            resultado = w;
            pthread_mutex_unlock(&w->mutex);
            break;
        }
        pthread_mutex_unlock(&w->mutex);
    }
    pthread_mutex_unlock(&lista_workers->mutex);
    return resultado;
}

void cambiar_estado_worker(worker_t* worker, estado_worker_t nuevo_estado) {
    pthread_mutex_lock(&worker->mutex);
    worker->estado = nuevo_estado;
    pthread_mutex_unlock(&worker->mutex);
}

void asignar_query_a_worker(worker_t* worker, int id_query) {
    pthread_mutex_lock(&worker->mutex);
    worker->query_asignada = id_query;
    worker->estado = WORKER_OCUPADO;
    pthread_mutex_unlock(&worker->mutex);
}

void liberar_worker(worker_t* worker) {
    pthread_mutex_lock(&worker->mutex);
    worker->query_asignada = -1;
    worker->estado = WORKER_LIBRE;
    pthread_mutex_unlock(&worker->mutex);
}

void notificar_asignacion_a_worker(worker_t* worker, query_t* query) {
    // TODO: aca se notifica al worker la asignacion de la query 
    
    enviar_operacion(worker->fd, ASIGNAR_QUERY);
    enviar_entero(worker->fd, master_logger, query->id);
    enviar_string(worker->fd, master_logger, query->path);
    enviar_entero(worker->fd, master_logger, query->pc);

    log_info(master_logger,"## Se envía la Query %d al Worker %d", query->id, worker->id);
}

void eliminar_worker_por_id(int id) {
    pthread_mutex_lock(&lista_workers->mutex);
    for (int i = 0; i < list_size(lista_workers->lista); i++) {
        worker_t* w = list_get(lista_workers->lista, i);
        if (w->id == id) {
            worker_t* eliminado = list_remove(lista_workers->lista, i);
            destruir_worker(eliminado);
            break;
        }
    }
    pthread_mutex_unlock(&lista_workers->mutex);
}

int obtener_cantidad_workers() {
    pthread_mutex_lock(&lista_workers->mutex);
    int cantidad = list_size(lista_workers->lista);
    pthread_mutex_unlock(&lista_workers->mutex);
    return cantidad;
}

void enviar_senal_desalojo(int id_worker, int id_query) {
    worker_t* worker = buscar_worker_por_id(id_worker);
    if (worker) {
        enviar_operacion(worker->fd, DESALOJAR_QUERY);
        enviar_entero(worker->fd,master_logger, id_query);
        log_info(master_logger, "Señal de desalojo enviada al Worker %d", worker->id);
    } else {
        log_warning(master_logger, "No se encontró el Worker %d para enviar señal de desalojo", id_worker);
    }
}