#include "../include/main.h"

int main(int argc, char** argv) {

    if (argc != 4) {
        fprintf(stderr, "Uso: %s <archivo_config> <archivo_query> <prioridad>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    char* archivo_config = argv[1];
    char* archivo_query = argv[2];
    int prioridad = atoi(argv[3]);


    inicializarQueryControl();
    iniciar_conexion_con_master(archivo_config, archivo_query, prioridad);

    // Crear hilo para atender al Master
    pthread_t hiloMaster;
    pthread_create(&hiloMaster, NULL, atender_master, (void*)&fd_conexion_master);
    pthread_join(hiloMaster,NULL);

    terminar_programa();

    return 0;
}
