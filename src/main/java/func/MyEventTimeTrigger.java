package func;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 *
 *  *
 * 知识补充
 * Flink官方提供了几种常用的trigger实现，同时，用户可以根据需求自定义trigger。
 * onElement() ，onProcessingTime()，onEventTime()方法的返回类型都是 TriggerResult
 *
 * onElement() ：方法会在窗口中每进入一条数据的时候调用一次
 * onProcessingTime()：方法会在一个ProcessingTime定时器触发的时候调用
 *
 * onEventTime()：方法会在一个EventTime定时器触发的时候调用
 *
 * clear()：方法会在窗口清除的时候调用
 *
 *
 * TriggerResult中包含四个枚举值：
 *
 * CONTINUE：表示对窗口不执行任何操作。
 * FIRE：表示对窗口中的数据按照窗口函数中的逻辑进行计算，并将结果输出。注意计算完成后，窗口中的数据并不会被清除，将会被保留。
 * PURGE：表示将窗口中的数据和窗口清除。
 * FIRE_AND_PURGE：表示先将数据进行计算，输出结果，然后将窗口中的数据和窗口进行清除。
 */
public class MyEventTimeTrigger extends Trigger<Object, TimeWindow> {
    //大多数代码是抄默认触发器的,根据需求改变 onElement 和onProcessingTime


    private static final long serialVersionUID = 1L;

    private MyEventTimeTrigger() {}
    //维护一个唯一的最终时间,只创建唯一的一个处理时间定时器
    Long processTimeTimer=null;

    /**
     * 每一个元素都会调用改方法
     * 我们的目的是用过processtime去触发窗口返回  TriggerResult
     * 所以需要创建一个处理时间定时器,然后过一段时间触发 onProcessingTime() 然后将窗口的结果返回
     *
     *
     *
     * @param element
     * @param timestamp
     * @param window
     * @param ctx
     * @return
     * @throws Exception
     */
    @Override
    public TriggerResult onElement(
            Object element, long timestamp, TimeWindow window, TriggerContext ctx)
            throws Exception {
        if (processTimeTimer!=null)
        ctx.deleteProcessingTimeTimer(processTimeTimer);
        //维护唯一最后的处理时间 设置十秒的定时
        processTimeTimer=System.currentTimeMillis()+10000;
        //注册一个处理时间定时器
        ctx.registerProcessingTimeTimer(processTimeTimer);
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {

        if(time == window.maxTimestamp()){

            return TriggerResult.FIRE;
        }else {

          return   TriggerResult.CONTINUE;
        }

    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
            throws Exception {
        
        System.out.println("触发输出结果");
        return TriggerResult.FIRE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) {
        // only register a timer if the watermark is not yet past the end of the merged window
        // this is in line with the logic in onElement(). If the watermark is past the end of
        // the window onElement() will fire and setting a timer here would fire the window twice.
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public String toString() {
        return "EventTimeTrigger()";
    }

    /**
     * Creates an event-time trigger that fires once the watermark passes the end of the window.
     *
     * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
     * trigger window evaluation with just this one element.
     */
    public static MyEventTimeTrigger create() {
        return new MyEventTimeTrigger();
    }

}
