#include "../include/main.h" 
#include "../include/controlador_query.h"

int main(int argc, char **argv)
{
    
    if (argc < 3)
    {
        fprintf(stderr, "Uso: ./worker [archivo_config] [ID_Worker]\n");
        // ./bin/worker worker.config 1
        return EXIT_FAILURE;
    }
    

    archivo_config = strdup(argv[1]); 
    worker_id = strdup(argv[2]);

    //archivo_config = "worker.config"; 
    //worker_id = "0";

    inicializar_worker(archivo_config); 

    log_info(worker_logger, "Iniciando Worker con archivo de configuracion: %s", archivo_config);
    log_debug(worker_logger, "Iniciando Worker con ID: %s", worker_id);

    iniciar_conexiones_iniciales(worker_id); 

    // Ciclo principal de escucha de instrucciones
    esperar_instrucciones_master();

    //free(archivo_config);
    //free(worker_id);
    terminar_programa();
    return EXIT_SUCCESS;
}
