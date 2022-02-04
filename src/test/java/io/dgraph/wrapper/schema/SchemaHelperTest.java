package io.dgraph.wrapper.schema;

import io.dgraph.DgraphClient;
import io.dgraph.wrapper.TestBase;
import io.dgraph.wrapper.model.DataType;
import io.dgraph.wrapper.model.StringIndexType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SchemaHelperTest extends TestBase {

  @Test
  public void testAlterPredicate() {
    DgraphClient client = null;
    try {
      client = getClient();
      SchemaHelper.alterPredicate(
          client, "bundleName", DataType.DT_STRING, true, StringIndexType.exact.name());
    } catch (Exception e) {
      e.printStackTrace();

      // never goes here
      Assert.assertTrue(false);
    } finally {
      if (null != client) {
        client.shutdown();
      }
    }
  }

  @Test
  public void testAlterType() {
    DgraphClient client = null;
    try {
      client = getClient();
      Bundle b = new Bundle();
      SchemaHelper.alterType(client, b.getDgraphType(), b.getPredicates());

    } catch (Exception e) {
      e.printStackTrace();

      // never goes here
      Assert.assertTrue(false);
    } finally {
      if (null != client) {
        client.shutdown();
      }
    }
  }

  @Test
  public void testAlter() {
    DgraphClient client = null;
    try {
      client = getClient();
      SchemaHelper.alter(client, test_schema);

    } catch (Exception e) {
      e.printStackTrace();

      // never goes here
      Assert.assertTrue(false);
    } finally {
      if (null != client) {
        client.shutdown();
      }
    }
  }

  String test_schema =
      "bundleName:string @index(exact) .\n"
          + "ext:string @index(exact) .\n"
          + "e1:string .\n"
          + "release_in:[uid] @reverse @count .\n"
          + "develop_in:[uid] @reverse .\n"
          + "\n"
          + "type Bundle {\n"
          + "  bundleName:string\n"
          + "  ext:string\n"
          + "  e1:string\n"
          + "  release_in:Country\n"
          + "  develop_in:Country\n"
          + "}\n"
          + "\n"
          + "country:string @index(exact) .\n"
          + "type Country{\n"
          + "\tcountry\n"
          + "}";
}
