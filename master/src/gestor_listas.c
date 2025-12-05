#include "../include/gestor_listas.h"

t_lista* crear_lista() {
    t_lista* nueva_lista = malloc(sizeof(t_lista));
    nueva_lista->lista = list_create();
    pthread_mutex_init(&nueva_lista->mutex, NULL);
    return nueva_lista;
}

void agregar_lista(t_lista* lista, void* elemento) {
    pthread_mutex_lock(&lista->mutex);
    list_add(lista->lista, elemento);
    pthread_mutex_unlock(&lista->mutex);
}

void* pop_lista(t_lista* lista) {
    pthread_mutex_lock(&lista->mutex);
    void* elemento = NULL;
    if (!list_is_empty(lista->lista)) {
        elemento = list_remove(lista->lista, 0);
    }
    pthread_mutex_unlock(&lista->mutex);
    return elemento;
}

bool esta_vacia(t_lista* lista) {
    pthread_mutex_lock(&lista->mutex);
    bool vacia = list_is_empty(lista->lista);
    pthread_mutex_unlock(&lista->mutex);
    return vacia;
}

void destruir_lista(t_lista* lista) {
    list_destroy(lista->lista);
    pthread_mutex_destroy(&lista->mutex);
    free(lista);
}

void destruir_lista_con_elementos(t_lista* lista, void (*destructor)(void*)) {
    pthread_mutex_lock(&lista->mutex);
    list_destroy_and_destroy_elements(lista->lista, destructor);
    pthread_mutex_unlock(&lista->mutex);
    pthread_mutex_destroy(&lista->mutex);
    free(lista);
}






