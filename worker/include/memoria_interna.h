#ifndef MEMORIA_INTERNA_H
#define MEMORIA_INTERNA_H

#include "gestor_worker.h"

//---------------------- INICIALIZACION/CREACION MEMORIA INTERNA ----------------------
void* inicializar_memoria_interna();
t_tabla_de_paginas* crear_tabla_paginas(char* file, char* tag, int cant_paginas);

//----------------------- LIBERACION/DESTRUCCION MEMORIA INTERNA -----------------------
void destruir_tabla_paginas(t_tabla_de_paginas* tabla);
void liberar_tabla_paginas_file_tag(char* file_tag_id);
void liberar_memoria_interna();
void liberar_marco(int nro_marco);

//----------------------- FUNCIONES DE MEMORIA INTERNA -----------------------
void aplicar_retardo_memoria();
int buscar_marco_libre();
void ocupar_marco(int nro_marco, t_tabla_de_paginas* tabla, int nro_pagina, const char* file, const char* tag);
t_marco* obtener_marco_de_pagina(char* file_tag_id, int nro_pagina);
t_tabla_de_paginas* obtener_tabla_paginas(char* file, char* tag);
bool crear_o_actualizar_tabla_paginas(char* file, char* tag, int tamanio);

// Manejo de Page Fault y Algoritmos
int manejar_page_fault(const char* file_tag, int nro_pagina, t_tabla_de_paginas* tabla);
int ejecutar_algoritmo_reemplazo();

extern pthread_mutex_t mutex_memoria;

#endif 