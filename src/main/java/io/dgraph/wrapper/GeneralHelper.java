package io.dgraph.wrapper;

import java.util.Collection;
import java.util.Iterator;

/** */
public class GeneralHelper {
  public static boolean isEmpty(String str) {
    return null == str || "".equals(str.trim());
  }

  /**
   * @param coll
   * @param split
   * @return
   */
  public static String implode(Collection<String> coll, String split) {
    StringBuffer buffer = new StringBuffer();
    if (null != coll && coll.size() > 0) {
      int i = 0;
      Iterator<String> iter = coll.iterator();
      while (iter.hasNext()) {
        if (i++ > 0) {
          buffer.append(split);
        }
        buffer.append(iter.next());
      }
    }
    return buffer.toString();
  }
}
