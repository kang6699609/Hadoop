import org.jeasy.rules.api.Action;
import org.jeasy.rules.api.Facts;

/**
 * @author yuanxiaokang
 * data 2020/9/25
 * 描述：
 */
public class DecreaseTemperatureAction implements Action {
    @Override
    public void execute(Facts facts) throws Exception {
        System.out.println("It is hot! cooling air..");
        Integer temperature = facts.get("temperature");
        facts.put("temperature", temperature - 1);
    }
    static DecreaseTemperatureAction decreaseTemperature() {
        return new DecreaseTemperatureAction();
    }
}
