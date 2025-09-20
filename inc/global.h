#ifndef _GLOBAL_H_
#define _GLOBAL_H_

#include <netinet/in.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "global.h"
#include <pthread.h>
#include <sys/select.h>
#include <arpa/inet.h>

/*发送窗口大小*/
#define SENDWND_SIZE 1024
/*初始化序列号*/
#define ISN 0
//定义server与client 地址
#define SERVER_IP "172.17.0.3"
#define CLIENT_IP "172.17.0.2"
//最大报文段生存时间
#define MSL 2000
// 单位是byte
#define SIZE32 4
#define SIZE16 2
#define SIZE8  1

// 一些Flag
#define NO_FLAG 0
#define NO_WAIT 1
#define TIMEOUT 2
#define TRUE 1
#define FALSE 0

// 定义最大包长 防止IP层分片
#define MAX_DLEN 1375 	// 最大包内数据长度
#define MAX_LEN 1400 	// 最大包长度

// TCP socket 状态定义
#define CLOSED 0
#define LISTEN 1
#define SYN_SENT 2
#define SYN_RECV 3
#define ESTABLISHED 4
#define FIN_WAIT_1 5
#define FIN_WAIT_2 6
#define CLOSE_WAIT 7
#define CLOSING 8
#define LAST_ACK 9
#define TIME_WAIT 10

// TCP 拥塞控制状态
#define SLOW_START 0
#define CONGESTION_AVOIDANCE 1
#define FAST_RECOVERY 2

// TCP 接受窗口大小
#define TCP_RECVWN_SIZE 32*MAX_DLEN // 比如最多放32个满载数据包

// TCP 发送窗口
// 注释的内容如果想用就可以用 不想用就删掉 仅仅提供思路和灵感
typedef struct {
	uint16_t window_size; //发送窗口大小
	uint32_t base;//发送窗口左边界 即base左侧数据包都已经被确认接收
	uint32_t nextseq; //下一个要发送的字节的序列号 序列号小于nextseq数据包都已经被发送（未必被确认）
	uint32_t estmated_rtt;//估算的往返时间
	int ack_cnt; //重复ACK计数器 实现快速重传
	pthread_mutex_t ack_cnt_lock; //ack_cnt互斥锁
	struct timeval send_time;//时间戳
	struct timeval timeout; //计算出的重传超时时间
	uint16_t rwnd; //接受方通告的窗口大小 从接受方发来的动态调整 用于流量控制
//	int congestion_status; 
//	uint16_t cwnd; 
//	uint16_t ssthresh; 
} sender_window_t;

// TCP 接受窗口
// 注释的内容如果想用就可以用 不想用就删掉 仅仅提供思路和灵感
typedef struct {
	char received[TCP_RECVWN_SIZE];

   //received_packet_t* head;
   char buf[TCP_RECVWN_SIZE];
   uint8_t marked[TCP_RECVWN_SIZE];
   uint32_t expect_seq;
} receiver_window_t;

// TCP 窗口 每个建立了连接的TCP都包括发送和接受两个窗口
typedef struct {
	sender_window_t* wnd_send;
  	receiver_window_t* wnd_recv;
} window_t;

typedef struct {
	uint32_t ip;
	uint16_t port;
} tju_sock_addr;


// TJU_TCP 结构体 保存TJU_TCP用到的各种数据
typedef struct {
	int state; // TCP的状态
	int to_be_free;//当前socket是否可被释放
	tju_sock_addr bind_addr; // 存放bind和listen时该socket绑定的IP和端口
	tju_sock_addr established_local_addr; // 存放建立连接后 本机的 IP和端口
	tju_sock_addr established_remote_addr; // 存放建立连接后 连接对方的 IP和端口

	pthread_mutex_t send_lock; // 发送数据锁
	char* sending_buf; // 发送数据缓存区
	int sending_len; // 发送数据缓存长度
	int send_index;//下一个写入位置偏移量
	int send_cleaned_len; //已被发送线程处理的数据长度
	pthread_mutex_t recv_lock; // 接收数据锁
	char* received_buf; // 接收数据缓存区
	int received_len; // 接收数据缓存长度

	pthread_cond_t wait_cond; // 可以被用来唤醒recv函数调用时等待的线程

	window_t window; // 发送和接受窗口
	/*缓存乱序报文*/
	char unorder[100][MAX_LEN];//乱序报文
	int unolen;//乱序报文数
	/*重传队列相关*/
	int packetr; // 队尾指针 (Rear pointer)，指向下一个空闲位置
    int packetf; // 队头指针 (Front pointer)，指向最早未确认的包
    pthread_mutex_t retrans_lock; // 保护重传队列的互斥锁

} tju_tcp_t;

#endif