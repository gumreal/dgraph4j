package io.dgraph.wrapper;

import io.dgraph.DgraphClient;
import org.testng.annotations.Test;

public class DgraphClientTest extends TestBase {
  @Test
  public void unitClientPerformance() throws Exception {
    long startMs = System.currentTimeMillis();
    DgraphClient client = getClient();
    client.shutdown();
    System.out.println(
        String.format(
            "first open/close dgraph client in %d ms", System.currentTimeMillis() - startMs));

    startMs = System.currentTimeMillis();
    client = getClient();
    client.shutdown();
    System.out.println(
        String.format(
            "second open/close dgraph client in %d ms", System.currentTimeMillis() - startMs));

    int times = 1000;
    startMs = System.currentTimeMillis();
    for (int i = 0; i < times; i++) {
      client = getClient();
      client.shutdown();
    }
    System.out.println(
        String.format(
            "open/close dgraph client %d times in %d ms",
            times, System.currentTimeMillis() - startMs));
  }
}
