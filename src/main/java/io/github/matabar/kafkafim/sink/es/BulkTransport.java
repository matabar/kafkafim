package io.github.matabar.kafkafim.sink.es;

import co.elastic.clients.transport.rest5_client.low_level.Request;
import co.elastic.clients.transport.rest5_client.low_level.Response;
import co.elastic.clients.transport.rest5_client.low_level.ResponseException;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import java.io.IOException;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;

/**
 * Sends one {@code _bulk} request and returns its status and body, whatever the status. Throws only
 * for transport failures (connection refused, timeout, TLS), which are request-level retryable. A
 * seam so the sender's retry logic is unit-testable without Elasticsearch.
 */
@FunctionalInterface
interface BulkTransport {

  /**
   * Sends {@code POST /_bulk}.
   *
   * @param body the NDJSON body
   * @return the HTTP status and response body
   * @throws IOException on a transport failure
   */
  Result send(byte[] body) throws IOException;

  /**
   * An HTTP answer.
   *
   * @param status HTTP status
   * @param body response body, possibly empty
   */
  record Result(int status, byte[] body) {}

  /** {@code application/x-ndjson}, what Elasticsearch documents for {@code _bulk}. */
  ContentType NDJSON = ContentType.create("application/x-ndjson");

  /**
   * A transport over the low-level client. The client picks a node (round robin, skipping dead
   * ones) and gzips the body when compression is on; on 502/503/504 or a connection error it tries
   * the next node before giving up.
   */
  static BulkTransport of(Rest5Client client) {
    return body -> {
      Request request = new Request("POST", "/_bulk");
      request.addParameter("filter_path", BulkResponse.FILTER_PATH);
      request.setEntity(new ByteArrayEntity(body, NDJSON));
      Response response;
      try {
        response = client.performRequest(request);
      } catch (ResponseException e) {
        response = e.getResponse();
      }
      return new Result(response.getStatusCode(), read(response.getEntity()));
    };
  }

  private static byte[] read(HttpEntity entity) throws IOException {
    return entity == null ? new byte[0] : EntityUtils.toByteArray(entity);
  }
}
