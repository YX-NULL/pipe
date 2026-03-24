### pipe_pipeline介绍

```text
用于构建多阶段串行处理流水线。它允许开发者通过一次函数调用,将多个独立的数据处理步骤串联起来,每个步骤在独立的线程中运行,从而实现高吞吐量的数据流处理。
```

### 函数原型

```c
pipeline_t pipe_pipeline(
    size_t first_size,      // [必选] 入口数据的单个元素大小
    proc_func, aux, out_sz, // [重复组] 阶段1: 处理函数, 辅助数据, 输出大小
    proc_func, aux, out_sz, // [重复组] 阶段2: ...
    ...                     // 可继续添加更多阶段
);
```

### 参数传递规则(三元组逻辑)

```text
该函数利用 C 语言的可变参数特性,遵循严格的参数序列:

first_size:定义流水线入口数据的单个元素字节大小。
后续参数:必须以(函数指针, 辅助指针, 输出大小) 为一组重复出现。
    函数指针(proc_func):处理逻辑,类型为(void)(const void, size_t, pipe_producer_, void)。
    辅助指针(aux):传递给处理函数的上下文数据(如配置结构体),可为 NULL。
    输出大小(out_sz):当前阶段处理后输出的数据元素大小。
```

### 数据流契约

```text
输入推导:第 N 个阶段的输入大小自动等于第 N-1 个阶段的输出大小(第一阶段输入大小即为 first_size)。
内存安全:处理函数内部调用 pipe_push 时,写入的数据块大小必须严格等于参数中声明的 out_sz。不匹配会导致内存越界或数据截断。

完整示例程序

以下示例模拟了一个数据处理工厂:
输入:原始整数(int)。
阶段 1:数值加倍,转换为 double。
阶段 2:格式化为字符串(char[64])。
阶段 3:添加前缀 "Result: "。
```

```c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "pipe.h" // 假设这是库的头文件

// --- 数据类型定义 ---
typedef int RawInt;
typedef double ProcessedDouble;
typedef char StringBuf[64]; // 固定大小字符串缓冲区

// --- 阶段 1: 整数 -> 双倍浮点数 ---
void stage_double(const void* in, size_t count, pipe_producer_t* out, void* aux) {
    const RawInt* input_vals = (const RawInt*)in;
    ProcessedDouble val;
    
    for (size_t i = 0; i < count; i++) {
        val = (double)input_vals[i] * 2.0;
        // 关键：推送大小必须匹配参数中的 sizeof(ProcessedDouble)
        pipe_push(out, &val, 1); 
    }
}

// --- 阶段 2: 浮点数 -> 字符串 ---
void stage_to_string(const void* in, size_t count, pipe_producer_t* out, void* aux) {
    const ProcessedDouble* input_vals = (const ProcessedDouble*)in;
    StringBuf buf;
    
    for (size_t i = 0; i < count; i++) {
        snprintf(buf, sizeof(StringBuf), "%.2f", input_vals[i]);
        pipe_push(out, buf, 1); 
    }
}

// --- 阶段 3: 字符串 -> 带前缀的字符串 ---
void stage_add_prefix(const void* in, size_t count, pipe_producer_t* out, void* aux) {
    const StringBuf* input_vals = (const StringBuf*)in;
    StringBuf buf;
    
    for (size_t i = 0; i < count; i++) {
        snprintf(buf, sizeof(StringBuf), "Result: %s", input_vals[i]);
        pipe_push(out, buf, 1);
    }
}

int main() {
    printf("=== 启动 pipe_pipeline 示例 ===\n");

    // 1. 构建流水线
    // 逻辑链: int -> [x2] -> double -> [sprintf] -> char[64] -> [prefix] -> char[64]
    pipeline_t pl = pipe_pipeline(
        sizeof(RawInt),                 // [起点] 输入是 int (4字节)
        
        stage_double, NULL, sizeof(ProcessedDouble), // 阶段1: 输出 double (8字节)
        stage_to_string, NULL, sizeof(StringBuf),    // 阶段2: 输出 char[64]
        stage_add_prefix, NULL, sizeof(StringBuf)    // 阶段3: 输出 char[64] (最终结果)
    );

    // 2. 生产者：推送数据
    printf("正在推送数据...\n");
    for (int i = 1; i <= 5; i++) {
        RawInt val = i;
        if (pipe_push(pl.in, &val, 1) != 0) {
            fprintf(stderr, "推送失败\n");
            break;
        }
        printf("已推送原始数据: %d\n", val);
    }

    // 关闭生产者，发送结束信号
    pipe_producer_free(pl.in); 

    // 3. 消费者：获取结果
    printf("\n正在接收结果...\n");
    StringBuf result;
    
    // 循环读取直到管道关闭且数据耗尽 (返回 0)
    while (pipe_pop(pl.out, &result, 1) > 0) {
        printf("收到最终结果: %s\n", result);
    }

    // 4. 清理资源
    pipe_consumer_free(pl.out);
    
    printf("=== 流水线结束 ===\n");
    return 0;
}
```

### 执行流程图解

```text
[主线程] 
   |
   |-- push(int) --> [管道1: elem_size=int] 
                      |
                      v
               [线程 A: stage_double] (读取 int, 计算, 推送 double)
                      |
                      |-- push(double) --> [管道2: elem_size=double]
                                           |
                                           v
                                    [线程 B: stage_to_string] (读取 double, 格式化, 推送 char[64])
                                           |
                                           |-- push(char[64]) --> [管道3: elem_size=char[64]]
                                                                    |
                                                                    v
                                                             [线程 C: stage_add_prefix] (读取, 加前缀, 推送)
                                                                    |
                                                                    |-- push(char[64]) --> [管道4: 最终缓冲]
                                                                                             |
                                                                                             v
[主线程] <--- pop(char[64]) -- (从这里读取最终结果)
```

### 关键注意事项

#### 大小匹配是生死线

```text
规则:pipe_pipeline 参数里的 sizeof 必须与函数内部 pipe_push 的实际 memcpy 长度完全一致。
后果:如果 stage_double 里 pipe_push 了 8 字节,但参数里写的是 sizeof(int)(4字节),程序会立即崩溃或产生随机垃圾数据。
```

#### 线程安全

```text
你的处理函数(stage_...) 运行在后台线程中。
如果使用了 aux 参数传递共享状态,必须自己加锁(mutex)。
管道本身的读写操作是线程安全的。
```

#### 阻塞与背压(Backpressure)

```text
pipe_push:如果下游管道满了,当前线程会阻塞,直到有空间。
pipe_pop:如果上游管道空了,当前线程会阻塞,直到有新数据或管道关闭。
这种机制天然形成了背压,防止快速的生产者撑爆内存,无需手动控制速率。
```

#### 资源释放顺序

```text
先关闭/释放生产者(pipe_producer_free),这会发送“结束信号”穿过整个流水线。
当所有数据处理完毕后,消费者(pipe_pop) 会返回 0。
最后释放消费者(pipe_consumer_free)。
中间的管道和线程通常由库在引用计数归零时自动回收。
```
