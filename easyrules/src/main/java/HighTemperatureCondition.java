import org.jeasy.rules.api.Condition;
import org.jeasy.rules.api.Facts;

/**
 * @author yuanxiaokang
 * data 2020/9/25
 * 描述：
 */
public class HighTemperatureCondition implements Condition {
    @Override
    public boolean evaluate(Facts facts) {
        Integer temperature = facts.get("temperature");
        return temperature > 25;
    }
    static HighTemperatureCondition itIsHot() {
        return new HighTemperatureCondition();
    }
}
