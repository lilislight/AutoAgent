## 现在的bug还以后的优化思路

### UI bugs
- 点击timeline的关闭并再次打开后，timeline的最大宽度被缩小，不是满屏

### core bugs
- 使用sqlite store时，顺序执行两个app.invoke,第二个invoke会遇到event loop的报错，
这是由于sqlite store的loop和执行loop是同一个，执行第一个invoke时数据库获取的锁没释放，导致第二次invoke报错。
可能的解决方案就是将数据库操作独立一个线程，和主线程隔离开。
- 关于Loop的bug，"LOOP_MULTIPLE_EDGES_ACTIVATED"，这个不应该，loop应该分为入口node和内部node，入口node持有外部入边和循环回边，现在的逻辑时对于外部入边会等待所有边都处理才ready，循环内回边只要处理过就可以ready，但是现在代码似乎限制了循环内的多边使用："LOOP_MULTIPLE_EDGES_ACTIVATED",需要解决一下，应该循环内部node和edge的行为和普通node，edge应该是一样的。

### 未来优化方案
- 删掉sqlite store，改为通用的database store，需要支持任意数据库，现在先兼容sqlite和postgresql（v1版本）
- app要支持同步接口和异步接口，并且可以随意调用而不要遇到event loop报错。
- WorkflowExecutor删掉同步方法，用不到。
- recovery的操作只放在app启动时，而不是放在invoke/submit等操作里面作为惰性recovery，invoke/submit只新建新的invocation，
遇到session里面存在invocation的状态为created，running，wait等，直接报错，说之前的还未完成。
- app只是执行workflow的class，app可以放到存在的fastapi里面作为一个方法；AutoAgentServer可以作为一个已经存在的fastapi router，
也可以单独run一个fastapi服务。app的生命周期管理不应该由AutoAgentServer来持有。

- 修改所有记录时间和计算耗时，都改成ns，当写event或者持久化时才转成ms的float，然后为input maaping/output binding/map/replcatin/等待线程池/等待调度/edge执行等等操作都记录耗时，不添加event，而是修改event的fileds，留下accureed_at_ms作为event发生时间，加一个offset_ms：float作为offset用于同一个发生时间的来判断先后，然后加一个timing，来存储所有的耗时，不同类型event只放存在的耗时字段。