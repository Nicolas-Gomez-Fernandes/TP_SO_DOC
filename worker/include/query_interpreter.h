#ifndef QUERY_INTERPRETER_H_
#define QUERY_INTERPRETER_H_

#include "gestor_worker.h"
#include "instrucciones_worker.h"
#include "controlador_query.h"

//===========================
//==== HEADERS FUNCIONES ====
//===========================
t_list* cargar_instrucciones(const char* full_path);
t_opcode parse_opcode(const char* linea);
char** parse_params(const char* linea, int* param_count);
void liberar_params(char** params, int param_count);
bool ejecutar_instruccion(t_opcode opcode, char** params, int param_count);
void notificar_master_fin_query(int query_id, const char* motivo_error_str);
void liberar_contexto();
void run_query_cycle();

#endif