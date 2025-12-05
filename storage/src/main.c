#include "../include/main.h"

int main(int argc, char **argv)
{

    if (argc < 2)
    {
        fprintf(stderr, "Uso: ./storage [archivo_config]\n");
        return EXIT_FAILURE;
    }


    char* archivo_config = strdup(argv[1]);

    inicializar_storage(archivo_config);

    log_info(storage_logger, "Iniciando Storage con archivo de configuracion: %s", archivo_config);

    atender_conexiones_storage();

    //free(archivo_config);
    terminar_programa();
    return EXIT_SUCCESS;
}