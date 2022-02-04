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
    return implode(coll, split, "", "");
  }

  /**
   * @param coll
   * @param split
   * @param itemPrefix
   * @param itemSuffix
   * @return
   */
  public static String implode(
      Collection<String> coll, String split, String itemPrefix, String itemSuffix) {
    return implode(coll, split, itemPrefix, itemSuffix, "", "");
  }

  /**
   * implode collection items to a single string <br>
   * for example<br>
   * List(a, b) = prefix + itemPrefix + "a" + itemSuffix + split + itemPrefix + "b" + itemSuffix +
   * suffix
   *
   * @param coll
   * @param split
   * @param itemPrefix
   * @param itemSuffix
   * @param prefix
   * @param suffix
   * @return
   */
  public static String implode(
      Collection<String> coll,
      String split,
      String itemPrefix,
      String itemSuffix,
      String prefix,
      String suffix) {
    StringBuffer buffer = new StringBuffer();
    buffer.append(prefix);

    if (null != coll && coll.size() > 0) {
      int i = 0;
      Iterator<String> iter = coll.iterator();
      while (iter.hasNext()) {
        if (i++ > 0) {
          buffer.append(split);
        }
        buffer.append(itemPrefix + iter.next() + itemSuffix);
      }
    }

    buffer.append(suffix);
    return buffer.toString();
  }

  /**
   * @param level
   * @return
   */
  public static String getIndentPrefix(int level) {
    StringBuffer buffer = new StringBuffer();
    for (int i = 0; i < level; i++) {
      buffer.append("    ");
    }
    return buffer.toString();
  }
}
