package func;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class MyTumblingEventTimeWindows extends TumblingEventTimeWindows {

    //构造器
    protected MyTumblingEventTimeWindows(long size, long offset, WindowStagger windowStagger) {
        super(size, offset, windowStagger);
    }

    //方法大多不需要改变只需要把原本默认的触发器,改成自己的触发器
    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
       return MyEventTimeTrigger.create();
    }
    // of方便创建自定义的窗口
    public static MyTumblingEventTimeWindows of(Time size ){
        return new MyTumblingEventTimeWindows(size.toMilliseconds(), 0, WindowStagger.ALIGNED);
    }
}
