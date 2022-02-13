package io.dgraph.wrapper.dql;

public class OrGroup extends SimpleGroup {
  /** @param notLogic */
  private OrGroup(boolean notLogic) {
    super(notLogic, Join.OR);
  }

  /**
   * @param notLogic
   * @param conditions
   * @return
   */
  public static SimpleGroup create(boolean notLogic, SimpleCondition... conditions) {
    return new OrGroup(notLogic).withConditions(conditions);
  }
}
