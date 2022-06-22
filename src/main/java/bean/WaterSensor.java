package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 水位传感器：用于接收水位数据
 * <p>
 * id:传感器编号
 * ts:时间戳
 * vc:水位
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;

    public static WaterSensor getInstance(String[] strings) {
        return new WaterSensor(strings[0], Long.parseLong(strings[1]), Integer.parseInt(strings[2]));
    }
}
