package cl.duoc.barriodigital.notify.amqp;

/**
 * Error que no se arregla reintentando (payload sin campos, comando mal formado):
 * el listener lo manda directo a la DLQ con basicNack(requeue=false), sin gastar reintentos.
 * Cualquier otra excepcion se considera recuperable (proveedor caido, timeout).
 */
public class ComandoNoRecuperableException extends RuntimeException {

    public ComandoNoRecuperableException(String message) {
        super(message);
    }
}
