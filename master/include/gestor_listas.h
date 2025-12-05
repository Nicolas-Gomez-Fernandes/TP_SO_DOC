#ifndef GESTOR_LISTAS_H
#define GESTOR_LISTAS_H

#include "gestor_master.h"

t_lista* crear_lista();
void agregar_lista(t_lista* lista, void* elemento);
void* pop_lista(t_lista* lista);
bool esta_vacia(t_lista* lista);
void destruir_lista(t_lista* lista);
void destruir_lista_con_elementos(t_lista* lista, void (*destructor)(void*));


#endif