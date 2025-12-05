#ifndef INSTRUCCIONES_WORKER_H_
#define INSTRUCCIONES_WORKER_H_

#include "gestor_worker.h"
#include "memoria_interna.h"
#include "query_interpreter.h"

//===========================
//==== HEADERS FUNCIONES ====
//===========================
bool execute_create(char **params, int param_count);
bool execute_truncate(char **params, int param_count);
bool execute_write(char **params, int param_count);
bool execute_read(char **params, int param_count);
bool execute_tag(char **params, int param_count);
bool execute_commit(char **params, int param_count);
bool execute_flush(char **params, int param_count);
bool execute_delete(char **params, int param_count);
bool execute_end_op(char **params, int param_count);

void realizar_flush_completo_memoria();
void iterador_guardar_paginas_modificadas(char *key, void *value);
#endif