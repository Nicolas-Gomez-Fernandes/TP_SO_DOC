#include "../include/utils.h"

t_log_level parse_log_level(const char* log_level_str) {
    if (strcmp(log_level_str, "TRACE") == 0) {
        return LOG_LEVEL_TRACE;
    } else if (strcmp(log_level_str, "DEBUG") == 0) {
        return LOG_LEVEL_DEBUG;
    } else if (strcmp(log_level_str, "INFO") == 0) {
        return LOG_LEVEL_INFO;
    } else if (strcmp(log_level_str, "WARNING") == 0) {
        return LOG_LEVEL_WARNING;
    } else if (strcmp(log_level_str, "ERROR") == 0) {
        return LOG_LEVEL_ERROR;
    } else {
        return LOG_LEVEL_ERROR;
    }
}

int crear_conexion(char *ip, char *puerto)
{
    struct addrinfo hints, *server_info, *p;
    int socket_cliente;
    int rv;


    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET; 
    hints.ai_socktype = SOCK_STREAM; 
    hints.ai_flags = AI_PASSIVE;

    // Obtener información del servidor
    if ((rv = getaddrinfo(ip, puerto, &hints, &server_info)) != 0) {
        fprintf(stderr, "Error en getaddrinfo: %s\n", gai_strerror(rv));
        return -1;
    }

    // Iterar sobre todas las direcciones posibles y conectarse a la primera disponible
    for(p = server_info; p != NULL; p = p->ai_next) {
        // Crear el socket
        socket_cliente = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (socket_cliente == -1) {
            perror("Error en socket");
            continue; 
        }

        // Conectar al socket
        if (connect(socket_cliente, p->ai_addr, p->ai_addrlen) == -1) {
            close(socket_cliente);
            perror("Error en connect socket, server esperado por socket no fue inciado");
            continue; 
        }

        break;
    }

    freeaddrinfo(server_info);

    // Verificar si se pudo conectar
    if (p == NULL) {
        return -1;
    }

    return socket_cliente;
}

int iniciar_servidor(char *puerto, t_log *un_log, char *msj_server)
{
	int socket_servidor;

	struct addrinfo hints, *servinfo;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;

	hints.ai_flags = AI_PASSIVE;	hints.ai_socktype = SOCK_STREAM;

	getaddrinfo(NULL, puerto, &hints, &servinfo);

 	// Creamos el socket de escucha del servidor
 	socket_servidor = socket(servinfo->ai_family,
                          servinfo->ai_socktype,
                          servinfo->ai_protocol);
	if (socket_servidor == -1) {
        perror("Error en crear socket de escucha");
        freeaddrinfo(servinfo);
        return -1;
    }
    int opt = 1;
    setsockopt(socket_servidor, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Asociar el socket a un puerto
    if (bind(socket_servidor, servinfo->ai_addr, servinfo->ai_addrlen) == -1) {
        perror("Error en bind");
        close(socket_servidor);
        freeaddrinfo(servinfo);
        return -1;
    }

    // Escuchar las conexiones entrantes
    if (listen(socket_servidor, SOMAXCONN) == -1) {
        perror("Error en listen");
        close(socket_servidor);
        freeaddrinfo(servinfo);
        return -1;
    }

	freeaddrinfo(servinfo);
	log_info(un_log, "[SERVER - %s]", msj_server);

	return socket_servidor;
}

int esperar_cliente(int socket_servidor, t_log *un_log, char *mensaje)
{
	// Aceptamos un nuevo cliente
	int socket_cliente = accept(socket_servidor, NULL, NULL);

	if (socket_cliente == -1)
	{
		perror("Error al aceptar la conexión");
		return -1;
	}
	log_info(un_log, "%s fd:%d", mensaje, socket_cliente);
	return socket_cliente;
}

int recibir_operacion(int socket_cliente)
{
	int cod_op;
    ssize_t bytes = recv(socket_cliente, &cod_op, sizeof(int), MSG_WAITALL);
	if (bytes > 0)
		return cod_op;
    else if (bytes == 0)
    {
        perror("El cliente cerro la conexion");
    }
	close(socket_cliente);
	return -1;

}

void enviar_operacion(int socket_fd, int cod_op) {
    ssize_t bytes = send(socket_fd, &cod_op, sizeof(int), MSG_NOSIGNAL);
    if (bytes <= 0) {
        perror("Error al enviar código de operación");
    }
}

int recibir_entero(int socket_fd, t_log* logger) {
    int valor;
    ssize_t bytes = recv(socket_fd, &valor, sizeof(int), MSG_WAITALL);
    if (bytes <= 0) {
        log_error(logger, "Error al recibir entero");
        return -1;
    }
    return valor;
}

void enviar_entero(int socket_fd, t_log* logger, int valor) {
    ssize_t bytes = send(socket_fd, &valor, sizeof(int), MSG_NOSIGNAL);
    if (bytes <= 0) {
        log_error(logger, "Error al enviar entero");
    }
}

void liberar_conexion(int socket_cliente)
{
	close(socket_cliente);
}

char* recibir_string(int socket_fd, t_log* logger) {
    int32_t longitud;

    if (recv(socket_fd, &longitud, sizeof(int32_t), MSG_WAITALL) <= 0) {
        log_error(logger, "Error al recibir la longitud del string");
        return NULL;
    }

    char* buffer = malloc(longitud);
    if (!buffer) {
        log_error(logger, "Error al reservar memoria para el string");
        return NULL;
    }

    if (recv(socket_fd, buffer, longitud, MSG_WAITALL) <= 0) {
        log_error(logger, "Error al recibir el string completo");
        free(buffer);
        return NULL;
    }

    return buffer;
}

void enviar_string(int socket_fd, t_log* logger, char* string) {
    ssize_t bytes;
    int32_t longitud = strlen(string) + 1;

    bytes = send(socket_fd, &longitud, sizeof(int32_t), MSG_NOSIGNAL);
    if (bytes <= 0) {
        log_error(logger, "Error enviando longitud del string");
        return;
    }

    bytes = send(socket_fd, string, longitud, MSG_NOSIGNAL);
    if (bytes <= 0) {
        log_error(logger, "Error enviando string");
        return;
    }
}

void protocolo_handshake_servidor(int socket) {
    int32_t handshake;
    op_code resultOK = HANDSHAKE_OK;
    op_code resultError = HANDSHAKE_ERROR;
    // int32_t largo_nombre;
    // void* nombre;

    recv(socket, &handshake, sizeof(int32_t), MSG_WAITALL);
    printf("Handshake recibido: %d\n", handshake);

    switch (handshake)
    {
    case HANDSHAKE:
        send(socket, &resultOK, sizeof(int32_t), MSG_NOSIGNAL);
        printf("Handshake sin argumento entro al case correcto\n");
        break;
        
    // case HANDSHAKE_IO:

    //     nombre = r(socket);
    //     printf("Nombre IO recibido: %s\n", (char*)nombre);


    //     //crear_estructura_IO(nombreIO) --> return bool

    //     send(socket, &resultOK, sizeof(int32_t), 0);
    //     printf("Handshake_io entro al case correcto\n");
    //     free(nombre);
    //     break;  
    
    // case HANDSHAKE_CPU:

    //     nombre = recibir_argumento_char(socket);
    //     printf("Nombre CPU recibido: %s\n", (char*)nombre);

    //     // crear_estructura_CPU(nombreCPU) --> return bool

    //     send(socket, &resultOK, sizeof(int32_t), 0);
    //     printf("Handshake_cpu entro al case correcto\n");
    //     free(nombre);

    //     break;

    default:
        send(socket, &resultError, sizeof(int32_t), MSG_NOSIGNAL);
        // liberar_recursos();
        break;
    }
}

void* recibir_buffer(int socket_fd, t_log* logger, int size) {
    void* buffer = malloc(size);
    if (buffer != NULL) {
        if (recv(socket_fd, buffer, size, MSG_WAITALL) != size) {
            log_error(logger, "Error recibiendo buffer. Se esperaban %d bytes.", size);
            free(buffer);
            return NULL;
        }
    }
    return buffer;
}

void enviar_buffer(int socket_fd, t_log* logger, void* buffer, int size) {
    if (send(socket_fd, buffer, size, MSG_NOSIGNAL) != size) {
        log_error(logger, "Error enviando buffer. Se intentaron enviar %d bytes.", size);
    }
}