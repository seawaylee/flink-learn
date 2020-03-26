# 启动Net输入流
nc -l 9000
# 提交flink任务
flink run --class WordCountDemo target/data-streaming-1.0-SNAPSHOT.jar