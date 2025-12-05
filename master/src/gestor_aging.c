#include "../include/gestor_aging.h"
static void* ciclo_aging_individual(void* arg) {
    query_t* query = (query_t*) arg;
    int query_id = query->id;
    while(true) {
        pthread_mutex_lock(&query->mutex_interno);
        if(!query->tiene_aging_activo) {
            pthread_mutex_unlock(&query->mutex_interno);
            break;
        }
        pthread_mutex_unlock(&query->mutex_interno);
        
        usleep(tiempo_again * 1000);
        
        pthread_mutex_lock(&query->mutex_interno);
        if(!query->tiene_aging_activo) {
            pthread_mutex_unlock(&query->mutex_interno);
            break;
        }
        
        // Hacer aging solo si sigue en READY
        if(query->estado == READY && query->prioridad > 0) {
            int prioridad_anterior = query->prioridad; // Se necesita guardar la prioridad anterior
            query->prioridad--;

            // Log Obligatorio de Cambio de Prioridad
            log_info(master_logger, "##%d Cambio de prioridad: %d %d", 
                query_id, prioridad_anterior, query->prioridad);

            //=========================
            sem_post(&sem_replanificar);
            //=======================
        } else if(query->estado != READY) {
            // La query cambió de estado, terminar
            query->tiene_aging_activo = false;
            pthread_mutex_unlock(&query->mutex_interno);
            break;
        } else {
            log_debug(master_logger, "Aging individual: Query %d ya tiene prioridad máxima (0)", query_id);
        }
        pthread_mutex_unlock(&query->mutex_interno);
    }
    
    log_info(master_logger, "Aging individual finalizado para Query %d", query_id);
    return NULL;
}

void crear_hilo_aging_para_query(query_t* query) {
    if (strcmp(algoritmo_planificacion, "PRIORIDADES") != 0) {
        return;
    }
    
    pthread_mutex_lock(&query->mutex_interno);
    
    // Verificar si ya tiene aging activo
    if(query->tiene_aging_activo) {
        pthread_mutex_unlock(&query->mutex_interno);
        return;
    }
    
    // Marcar como activo
    query->tiene_aging_activo = true;

    // Crear el hilo pasando directamente la query
    pthread_create(&query->hilo_aging, NULL, ciclo_aging_individual, query);
    pthread_detach(query->hilo_aging);
    
    pthread_mutex_unlock(&query->mutex_interno);
    log_info(master_logger, "Hilo de aging creado para Query %d", query->id);
}

void detener_aging_para_query(query_t* query) {
    pthread_mutex_lock(&query->mutex_interno);
    
    if(query->tiene_aging_activo) {
        log_info(master_logger, "Deteniendo aging para Query %d", query->id);
        query->tiene_aging_activo = false;
        // El hilo terminará automáticamente en su próxima iteración
    }
    
    pthread_mutex_unlock(&query->mutex_interno);
}