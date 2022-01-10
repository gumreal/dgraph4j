package io.dgraph.wrapper;

import io.dgraph.DgraphClient;
import io.dgraph.DgraphGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Create client of DGraph */
public class ClientBuilder {
  private Logger logger = LoggerFactory.getLogger(ClientBuilder.class.getName());
  private Set<String> uriSet = new HashSet<>();
  private List<AlphaUri> alphaList = new ArrayList<>();

  private ClientBuilder() {}

  public static ClientBuilder newInstance() {
    return new ClientBuilder();
  }

  /**
   * @param host
   * @param port
   * @return
   * @throws WrapperException
   */
  public ClientBuilder withAlpha(String host, int port) throws WrapperException {
    if (GeneralHelper.isEmpty(host) || port <= 0) {
      throw new WrapperException("invalid alpha uri");
    }

    String key = String.format("%s:%d", host, port);
    if (uriSet.contains(key)) {
      logger.info("duplicated alpha " + key);
    } else {
      alphaList.add(new AlphaUri(host, port));
    }

    return this;
  }

  /**
   * @return
   * @throws WrapperException
   */
  public DgraphClient build() throws WrapperException {
    if (alphaList.size() == 0) {
      throw new WrapperException("no alpha");
    }

    DgraphGrpc.DgraphStub[] arr = new DgraphGrpc.DgraphStub[alphaList.size()];
    for (int i = 0; i < alphaList.size(); i++) {
      AlphaUri uri = alphaList.get(i);
      arr[i] = makeStub(uri.getHost(), uri.getPort());
    }
    return new DgraphClient(arr);
  }

  /**
   * @param addr
   * @param port
   * @return
   */
  private DgraphGrpc.DgraphStub makeStub(String addr, int port) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress(addr, port).usePlaintext().build();
    return DgraphGrpc.newStub(channel);
  }

  /** Alpha URI: host and port */
  protected static class AlphaUri {
    private String host;
    private int port;

    public AlphaUri(String host, int port) {
      this.host = host;
      this.port = port;
    }

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }
  }
}
