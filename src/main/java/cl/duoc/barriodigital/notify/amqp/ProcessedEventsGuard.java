package cl.duoc.barriodigital.notify.amqp;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// ponytail: dedupe en memoria de una sola instancia. Si notify-svc escala a
// mas de una replica, esto deja de servir y hay que pasar a una tabla/Redis
// con TTL por eventId.
@Component
public class ProcessedEventsGuard {

    private final Set<String> vistos = ConcurrentHashMap.newKeySet();

    /** true si es la primera vez que se ve este eventId (y lo marca como visto). */
    public boolean esNuevo(String eventId) {
        return vistos.add(eventId);
    }
}
