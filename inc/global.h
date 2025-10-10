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
#include <stdbool.h>
/*MIN与MAX*/
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))
/*缓冲区设置*/
#define MAX_PKT_IN_WND 100
#define MAX_BUF_SIZE 14000
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
#define MAX_WND_SIZE	32*MAX_DLEN
/*受限的SR实现*/
typedef struct{
	uint32_t seq_num; //序列号
	char* data;		  //数据
	size_t data_len;	//数据长度
	struct timeval send_time;	//发送时间
	bool acked;			//是否已确认
	int retransmit_count;	//重传次数	
}sr_packet_t;
/*RTT统计与重传控制*/
typedef struct{
	/*RTT统计数据*/
   	struct timeval send_time;
   	struct timeval timeout;
	//RTT状态统计
	int rtt_initialized;
	int retransmitted_in_flight;
	//RTT估计值 
	long estmated_rtt;   // microseconds - 平滑RTT
    long dev_rtt;   
	long estimated_rtt_ms; //平滑RTT
	long dev_rtt_ms ;	//RTT偏差
	long timeout_interval_ms; //超时间隔

	long rto_ms;

}rtt_stats_t;
// TCP 发送窗口
typedef struct {
	uint16_t window_size;
    uint32_t base;
   	uint32_t nextseq;
   	uint32_t estmated_rtt;
	int same_ack_cnt;
   	int ack_cnt;
   	pthread_mutex_t ack_cnt_lock;
	// 重传统计
	int retransmitted_in_flight;
    uint32_t total_retransmissions;
    uint32_t timeout_retransmissions;
    uint32_t fast_retransmissions;
	struct timeval send_time;
   	struct timeval timeout;
	//SR实现
	sr_packet_t* packets[MAX_WND_SIZE];
	pthread_mutex_t mutex;//发送包的缓存锁
   	uint16_t rwnd;
   	//int congestion_status;
  	//uint16_t cwnd; 
   	//uint16_t ssthresh; 
} sender_window_t;

// TCP 接受窗口
// 注释的内容如果想用就可以用 不想用就删掉 仅仅提供思路和灵感
typedef struct {
	char received[TCP_RECVWN_SIZE];

//   received_packet_t* head;
   	char buf[TCP_RECVWN_SIZE];
   	uint8_t marked[TCP_RECVWN_SIZE];//用于缓存
	uint32_t avail_wnd_size;
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
	int send_cleaned_len; //已经发送且确认数据长度

	pthread_mutex_t recv_lock; // 接收数据锁
	char* received_buf; // 接收数据缓存区
	int received_len; // 接收数据缓存长度
	int recv_cleaned_len;//已经接收且处理数据

	pthread_cond_t wait_cond; // 可以被用来唤醒recv函数调用时等待的线程

	window_t window; // 发送和接受窗口
	char* packet_FIN;//重传包
	rtt_stats_t rtt_stats;
	/*管理快速重传*/
	int fast_retransmit;
	uint32_t fast_retransmit_seq;//需要快速重传的seq
	/*管理乱序pkt*/
	char unorder[100][MAX_LEN];
	int unolen;//乱序报文个数
} tju_tcp_t;

#endif