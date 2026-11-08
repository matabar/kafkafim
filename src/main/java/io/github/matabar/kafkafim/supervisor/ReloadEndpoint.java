package io.github.matabar.kafkafim.supervisor;

import java.util.List;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;

/**
 * {@code POST /actuator/reload} (DESIGN §5.6): the same reload the file watcher runs, on demand.
 * Waits for the reload, including the drains of removed and restarted pipelines, and returns a
 * {@link ReloadResult}: 200 when the tree was applied or unchanged, 400 with every violation when
 * it was rejected (the running configuration stays), 503 while the supervisor is not running.
 *
 * <p>It lives in {@code supervisor} rather than {@code telemetry} because it drives the supervisor,
 * and nothing may depend on {@code supervisor} (ADR-0003): a {@code telemetry} endpoint calling
 * into {@code supervisor} would close a {@code telemetry} ↔ {@code supervisor} cycle as soon as the
 * supervisor uses telemetry's meters.
 */
@Endpoint(id = "reload")
public class ReloadEndpoint {

  private final ConfigReloader reloader;

  ReloadEndpoint(ConfigReloader reloader) {
    this.reloader = reloader;
  }

  /** Reloads the configuration from disk now. */
  @WriteOperation
  public WebEndpointResponse<ReloadResult> reload() {
    ReloadResult result;
    try {
      result = reloader.reload("endpoint");
    } catch (IllegalStateException e) {
      return new WebEndpointResponse<>(
          new ReloadResult(
              ReloadResult.REJECTED,
              reloader.generation(),
              List.of(),
              List.of(),
              List.of(),
              List.of(),
              List.of(e.getMessage())),
          WebEndpointResponse.STATUS_SERVICE_UNAVAILABLE);
    }
    return new WebEndpointResponse<>(
        result,
        result.accepted() ? WebEndpointResponse.STATUS_OK : WebEndpointResponse.STATUS_BAD_REQUEST);
  }
}
