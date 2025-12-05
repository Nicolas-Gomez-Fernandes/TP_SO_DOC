#include "../include/main.h"

void test_planificacion_simple();
void test_stress_fifo();
void test_planificador_y_desalojo_worker();
void test_planificador_y_desalojo_query();

int main(int argc, char* argv[]) {

    inicializar_master();

    iniciar_planificador();

    terminar_programa();

    return 0;
}

void test_planificacion_simple() {

    // Crear un worker de prueba
    crear_worker(1, 100); // id=1, fd=100 (dummy)
    
    // Crear una query de prueba
    crear_query("ruta/al/archivo", 2, 10); // path, prioridad, fd_query_control

    // Simular ciclo de planificación
    worker_t* worker_libre = buscar_worker_libre();
    query_t* query_ready = buscar_query_ready();

    if (worker_libre && query_ready) {
        // Asignaciones
        asignar_worker_a_query(query_ready, worker_libre->id);
        asignar_query_a_worker(worker_libre, query_ready->id);

        log_info(master_logger, "Test: Worker %d asignado a Query %d", worker_libre->id, query_ready->id);
    } else {
        log_info(master_logger, "Test: No se pudo asignar worker o query");
    }

}

void test_stress_fifo() {
    int cantidad_workers = 2;
    int cantidad_queries = 1;

    // Crear workers
    for (int i = 1; i <= cantidad_workers; i++) {
        crear_worker(i, 100 + i); // id, fd dummy
    }

    // Crear queries FIFO (todas con la misma prioridad)
    for (int i = 1; i <= cantidad_queries; i++) {
        // Prioridad fija para forzar FIFO puro
        crear_query("ruta/al/archivo", 1, 1000 + i); // path, prioridad, fd_query_control dummy
        log_info(master_logger, "Test FIFO: Query creada con ID esperado %d", i-1); // Suponiendo que el id_query_global arranca en 0
    }

    iniciar_planificador();
}


void test_planificador_y_desalojo_worker() {
    // -------------------------------------------
    // Precondiciones: se debe poner un slepp de 
    //                  3seg en el atender worker para simular la comunicacion
    // -------------------------------------------

    log_info(master_logger, "=== Test: Planificador y desalojo de Worker ===");


    int fd_worker = 127;
    int fd_query_control = 100;

    // Crear y almacenar el Worker real
    int id_worker = 2;
    worker_t* worker = crear_worker(id_worker, fd_worker);

    // Crear hilo para atender al Worker
    pthread_t hiloWorker;
    pthread_create(&hiloWorker, NULL, atender_worker, (void*)worker);
    pthread_detach(hiloWorker);

    // Crear y almacenar la query real
    char* query_path = strdup("path");
    int priority = 1;
    query_t* nueva_query = crear_query(query_path, priority, fd_query_control);
    

    // Crear hilo para atender al Query Control
    pthread_t hiloQuery;
    pthread_create(&hiloQuery, NULL, atender_query, (void*)nueva_query);
    pthread_detach(hiloQuery);

    log_info(master_logger, "Cantidad de workers antes desalojo de query: %d", obtener_cantidad_workers());
    log_info(master_logger, "Cantidad de queries antes desalojo de query: %d", obtener_cantidad_querys());

    // Iniciar el planificador en un hilo aparte
    iniciar_planificador();

    // Esperar a que el planificador asigne la query
    sleep(2);

    // Simular desconexión del Worker
    log_info(master_logger, "Simulando desconexión de Worker %d...", worker->id);
    close(fd_worker); // Esto debería disparar el case -1 en atender_worker

    // Esperar a que el planificador procese la desconexión
    sleep(1);

    // Verificar estado
    int cant_workers = obtener_cantidad_workers();
    int cant_querys = obtener_cantidad_querys();
    log_info(master_logger, "Cantidad de workers tras desconexión: %d", cant_workers);
    log_info(master_logger, "Cantidad de queries tras desconexión: %d", cant_querys);

    detener_planificador();

    log_debug(master_logger, "Test Finalizado con exito!!");
}

void test_planificador_y_desalojo_query() {
    // -------------------------------------------
    // Precondiciones: se debe poner un slepp de 
    //                  3seg en el atender query para simular la comunicacion
    // -------------------------------------------
    log_info(master_logger, "=== Test: Planificador y desalojo de Query Control ===");

    int fd_worker = 128;
    int fd_query_control = 101;

    // Crear y almacenar el Worker real
    int id_worker = 3;
    worker_t* worker = crear_worker(id_worker, fd_worker);

    // Crear hilo para atender al Worker
    pthread_t hiloWorker;
    pthread_create(&hiloWorker, NULL, atender_worker, (void*)worker);
    pthread_detach(hiloWorker);

    // Crear y almacenar la query real
    char* query_path = strdup("path_query");
    int priority = 2;
    query_t* nueva_query = crear_query(query_path, priority, fd_query_control);

    // Crear hilo para atender al Query Control
    pthread_t hiloQuery;
    pthread_create(&hiloQuery, NULL, atender_query, (void*)nueva_query);
    pthread_detach(hiloQuery);

    log_info(master_logger, "Cantidad de workers antes desalojo de query: %d", obtener_cantidad_workers());
    log_info(master_logger, "Cantidad de queries antes desalojo de query: %d", obtener_cantidad_querys());

    // Iniciar el planificador en un hilo aparte
    iniciar_planificador();

    // Esperar a que el planificador asigne la query
    sleep(2);

    // Simular desconexión del Query Control
    log_info(master_logger, "Simulando desconexión de Query Control (fd %d)...", fd_query_control);
    close(fd_query_control); // Esto debería disparar el case -1 en atender_query

    // Esperar a que el planificador procese la desconexión
    sleep(8);

    // Verificar estado
    int cant_workers = obtener_cantidad_workers();
    int cant_querys = obtener_cantidad_querys();
    log_info(master_logger, "Cantidad de workers tras desalojo de query: %d", cant_workers);
    log_info(master_logger, "Cantidad de queries tras desalojo de query: %d", cant_querys);

    detener_planificador();

    log_debug(master_logger, "Test de desalojo de Query Control finalizado con éxito!!");
}
