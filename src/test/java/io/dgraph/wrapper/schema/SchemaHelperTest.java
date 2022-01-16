package io.dgraph.wrapper.schema;

import io.dgraph.wrapper.TestBase;
import io.dgraph.wrapper.model.DataType;
import io.dgraph.wrapper.model.StringIndexType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SchemaHelperTest extends TestBase {

  @Test
  public void testAlterPredicate() {
    try {
      SchemaHelper.alterPredicate(
          getClient(), "bundleName", DataType.DT_STRING, true, StringIndexType.exact.name());
    } catch (Exception e) {
      e.printStackTrace();

      // never goes here
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testAlterType() {
    try {
      Bundle b = new Bundle();
      SchemaHelper.alterType(getClient(), b.getDgraphType(), b.getPredicates());

    } catch (Exception e) {
      e.printStackTrace();

      // never goes here
      Assert.assertTrue(false);
    }
  }
}
