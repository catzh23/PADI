package didatrade.util;

public class DebugMode {
  public static final int NONE = 0;
  public static final int FAIL = 1;
  public static final int FREEZE = 2;
  public static final int UNFREEZE = 3;
  public static final int SLOW = 4;
  public static final int FAST = 5;

  public static final String USAGE = "crash | freeze | un-freeze | slow-mode-on | slow-mode-off";

  /** Accepts a mode name or a mode number. Returns -1 when the mode is unknown. */
  public static int parse(String mode) {
    switch (mode.toLowerCase()) {
      case "crash":
      case "fail":
        return FAIL;
      case "freeze":
        return FREEZE;
      case "un-freeze":
      case "unfreeze":
        return UNFREEZE;
      case "slow-mode-on":
      case "slow":
        return SLOW;
      case "slow-mode-off":
      case "fast":
        return FAST;
      default:
        break;
    }
    try {
      int number = Integer.parseInt(mode);
      return (number >= FAIL && number <= FAST) ? number : -1;
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  public static String name(int mode) {
    switch (mode) {
      case FAIL:
        return "crash";
      case FREEZE:
        return "freeze";
      case UNFREEZE:
        return "un-freeze";
      case SLOW:
        return "slow-mode-on";
      case FAST:
        return "slow-mode-off";
      default:
        return "unknown";
    }
  }
}
