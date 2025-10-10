#include "tju_tcp.h"
#include <unistd.h>
#include <stdlib.h>
static int send_thread_created = 0;
int resend_thread_created=0;

static FILE* g_trace_fp=NULL;
static int g_is_server=0;//server 1 client 0

/*程序退出时清理trace文件*/
void cleanup_trace_file(void) {
    if (g_trace_fp) {
        fflush(g_trace_fp); // 退出时确保数据写入
        fclose(g_trace_fp);
        g_trace_fp = NULL;
    }
}
/*得到时间戳*/
static long getCurrentTime(){
    struct  timeval tv;
    gettimeofday(&tv,NULL);
    return tv.tv_sec*1000000+tv.tv_usec;    
}
/*文件初始化*/
static void open_trace_file(){
    if (g_trace_fp) return;  // 如果文件已打开，直接返回
    
    char hostname[8];
    gethostname(hostname, 8);
    g_is_server = (strcmp(hostname, "server") == 0);  // 根据主机名判断角色
    
    const char* dir = "/vagrant/tju_tcp/test";
    // 创建目录（容忍失败）
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "mkdir -p %s", dir);
    system(cmd);
    
    // 根据角色选择不同的文件路径
    const char* path = g_is_server ? 
        "/vagrant/tju_tcp/test/server.event.trace" : 
        "/vagrant/tju_tcp/test/client.event.trace";
    
    g_trace_fp = fopen(path, "w"); // 覆盖模式打开
    if (g_trace_fp == NULL) {
        // 退化：如果指定路径失败，尝试当前目录
        path = g_is_server ? "server.event.trace" : "client.event.trace";
        g_trace_fp = fopen(path, "w");
    }
    
    // 注册退出时清理函数
    static int cleanup_registered = 0;
    if (!cleanup_registered) {
        atexit(cleanup_trace_file);
        cleanup_registered = 1;
    }
}
/*trace初始化*/
void trace_init(){
    open_trace_file();
}
/*标志位转换*/
static int flag_to_num(uint8_t flags){
    switch (flags)
    {
    case NO_FLAG:
        return 0;
    case FIN_FLAG_MASK:
        return 2;
    case ACK_FLAG_MASK:
        return 4;
    case SYN_FLAG_MASK:
        return 8;
    case (SYN_FLAG_MASK|ACK_FLAG_MASK):
        return 12;
    default:
        return 1;
    }
}
/*将所有获取信息写入文件*/
static void trace_line(const char* event, const char* info){
    //确保文件已经打开 直接写入
    open_trace_file();
    if(!g_trace_fp) return;

    long time=getCurrentTime();
    fprintf(g_trace_fp,"[%ld] [%s] [%s]\n",time,event,info);
    // 移除fflush，减少磁盘I/O开销
}
/*数据包发送追踪*/
void trace_send(char* pkt){
    uint32_t seq=get_seq(pkt);
    uint32_t ack=get_ack(pkt);
    uint8_t flags=get_flags(pkt);
    uint16_t plen=get_plen(pkt);
    uint16_t hlen=get_hlen(pkt);
    int flagnum=flag_to_num(flags);
    int len=(int)(plen-hlen);   

    char info[256];
    snprintf(info,sizeof(info),"seq:%u ack:%u flag:%d length:%d", seq, ack, flagnum, len);
    trace_line("SEND",info);
}
/*数据包接收追踪 与发送类似*/
void trace_recv(char* pkt){
    int32_t seq=get_seq(pkt);
    uint32_t ack=get_ack(pkt);
    uint8_t flags=get_flags(pkt);
    uint16_t plen=get_plen(pkt);
    uint16_t hlen=get_hlen(pkt);
    int flagnum=flag_to_num(flags);
    int len=(int)(plen-hlen);   

    char info[256];
    snprintf(info,sizeof(info),"seq:%u ack:%u flag:%d length:%d", seq, ack, flagnum, len);
    trace_line("RECV",info);
}
/*窗口大小追踪*/
/*接收窗口*/
void trace_rwnd(uint32_t size){
    char info[64];
    snprintf(info,sizeof(info),"size:%u",size);
    trace_line("RWND",info);
}
/*发送发送窗口*/
void trace_swnd(uint32_t size){
     char info[64];
    snprintf(info,sizeof(info),"size:%u",size);
    trace_line("SWND",info);
}
/*拥塞窗口*/
void trace_cwnd(int type, uint32_t size){
    char info[64];
    snprintf(info,sizeof(info),"type:%d size:%u",type,size);
    trace_line("CWND",info);
}
/*RTT统计跟踪*/
void trace_rtts(double sample_ms,double estimate_ms,double dev_ms,double timeout_ms){
    char info[256];
    snprintf(info,sizeof(info),"SampleRTT:%f EstimatedRTT:%f DeviationRTT:%f TimeoutInterval:%f",sample_ms,estimate_ms,dev_ms,timeout_ms);
    trace_line("RTTS",info);
}
/*数据交付追踪*/
void trace_delv(uint32_t seq,uint32_t size){
    char info[128];
    snprintf(info,sizeof(info),"seq:%u size:%u",seq,size);
    trace_line("DELV",info);
}

/*兼容性函数*/
void init_log_file(int server_mode) {
    trace_init();
}

void close_log_file() {
    cleanup_trace_file();
}

void trace_init_log(){
    trace_init();
}
/*
创建 TCP socket 
初始化对应的结构体
设置初始状态为 CLOSED
*/
tju_tcp_t* tju_socket(){
    tju_tcp_t* sock = (tju_tcp_t*)malloc(sizeof(tju_tcp_t));
    sock->state = CLOSED;
    /*初始化发送窗口*/
    if (sock->window.wnd_send == NULL) {
        sock->window.wnd_send = malloc(sizeof(sender_window_t));
    }
    sender_window_t* send_win=sock->window.wnd_send;
    send_win->base=0;//初始化为0
    send_win->nextseq=send_win->base;
    send_win->window_size=MAX_WND_SIZE;
    //初始化pkt缓存数组
    for (int i = 0; i <MAX_WND_SIZE; i++) {
        send_win->packets[i] = NULL;
    }
    pthread_mutex_init(&send_win->mutex, NULL);
    /*初始化接收窗口*/
    sock->window.wnd_recv=malloc(sizeof(receiver_window_t));
    receiver_window_t* recv_win=sock->window.wnd_recv;
    recv_win->expect_seq=0;//期望接收的序列号从0开始
    recv_win->avail_wnd_size=TCP_RECVWN_SIZE;//可用窗口大小
    /*初始化pkt缓存数组*/
    sock->unolen=0;//只要置0即可
    
    pthread_mutex_init(&(sock->send_lock), NULL);
    sock->sending_buf = NULL;
    sock->sending_len = 0;
    sock->send_cleaned_len=0;

    pthread_mutex_init(&(sock->recv_lock), NULL);
    sock->received_buf = NULL;
    sock->received_len = 0;
    sock->recv_cleaned_len=0;
    if(pthread_cond_init(&sock->wait_cond, NULL) != 0){
        perror("ERROR condition variable not set\n");
        exit(-1);
    }
    /*初始化RTT统计数据*/
    sock->rtt_stats.rtt_initialized=0;
    sock->rtt_stats.retransmitted_in_flight=0;
    sock->rtt_stats.estimated_rtt_ms=0;
    sock->rtt_stats.dev_rtt=0;
    sock->rtt_stats.estimated_rtt_ms=0;
    sock->rtt_stats.dev_rtt_ms=0;
    sock->rtt_stats.timeout_interval_ms=3000;//debug 初始时间间隔设置为3s
    sock->rtt_stats.rto_ms=3000;    //一样的量 无需多言
    /*初始化快速重传相关*/
    sock->fast_retransmit=0;
    sock->fast_retransmit_seq=0;
    
    trace_init(sock);
    return sock;
}

/*
绑定监听的地址 包括ip和端口
*/
int tju_bind(tju_tcp_t* sock, tju_sock_addr bind_addr){
    //这里存储分配的绑定端口
    sock->bind_addr = bind_addr;
    return 0;
}

/*
被动打开 监听bind的地址和端口
设置socket的状态为LISTEN
注册该socket到内核的监听socket哈希表
*/
int tju_listen(tju_tcp_t* sock){
    sock->state = LISTEN;
    int hashval = cal_hash(sock->bind_addr.ip, sock->bind_addr.port, 0, 0);
    listen_socks[hashval] = sock;
    return 0;
}

/*
接受连接 
返回与客户端通信用的socket
这里返回的socket一定是已经完成3次握手建立了连接的socket
因为只要该函数返回, 用户就可以马上使用该socket进行send和recv
*/
tju_tcp_t* tju_accept(tju_tcp_t* listen_sock){
    int flag=0;
    tju_tcp_t* new_conn=NULL;
    //阻塞等待，从全连接队列中返回一个可用socket
    //printf("服务端:等待客户端连接\n");
    while(!flag){
        for(int i=0;i<MAX_SOCK;i++){
            if(acceptqueue[i]!=NULL&&acceptqueue[i]->state==ESTABLISHED){
                new_conn=acceptqueue[i];
                //printf("成功找到已建立连接的socket，索引=%d\n", i);
                acceptqueue[i]=NULL;//从列表中移除
                flag=1;
                break;
            }
        }
        if(!flag){
            usleep(10000);//休眠10ms避免忙等待
        }
    }
    //printf("accept返回socket: %p, 状态: %d\n", new_conn, new_conn->state);
    return new_conn;
}


/*
连接到服务端
该函数以一个socket为参数
调用函数前, 该socket还未建立连接
函数正常返回后, 该socket一定是已经完成了3次握手, 建立了连接
因为只要该函数返回, 用户就可以马上使用该socket进行send和recv
*/
int tju_connect(tju_tcp_t* sock, tju_sock_addr target_addr){

    sock->established_remote_addr = target_addr;

    tju_sock_addr local_addr;
    local_addr.ip = inet_network(CLIENT_IP);
    //local_addr.port = 5678; 要随机分配一个端口
    local_addr.port = random_port();
    sock->established_local_addr = local_addr;
    /*第一次握手*/
    //向客户端发送SYN
    uint32_t seq=ISN;//初始化序列号 ISN
    uint32_t ack=0;//其实这里应该没有ack的
    char* syn_flags=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, SYN_FLAG_MASK, 1, 0, NULL, 0);
    sendToLayer3(syn_flags,DEFAULT_HEADER_LEN);
    sock->state=SYN_SENT;
    trace_send(syn_flags);
    // 将建立了连接的socket放入内核 已建立连接哈希表中
    int hashval = cal_hash(local_addr.ip, local_addr.port, target_addr.ip, target_addr.port);
        established_socks[hashval] = sock;
    //简单的超时重传
    struct timeval start,current;
    long timeout=3;
    gettimeofday(&start,NULL);
    while(sock->state!=ESTABLISHED){
        gettimeofday(&current,NULL);
        long elapsed = (current.tv_sec - start.tv_sec) +   
                       (current.tv_usec - start.tv_usec) / 1000000.0; 
        if(elapsed>timeout){
            //超时重传
            sendToLayer3(syn_flags,DEFAULT_HEADER_LEN);
            gettimeofday(&start,NULL);//将start重置为当前时间
        }
    }
    return 0;
}
int tju_send(tju_tcp_t* sock, const void *buffer, int len){
    //原先的具体发送部分在发送线程中实现
    //确定只有一个tcp连接 可以直接在全局中设置thread-flag
    //检查TCP状态是否可发送
    if(sock->state!=ESTABLISHED&&sock->state!=CLOSE_WAIT){
        //注意 close-wait阶段服务器会将剩余数据确认等传输回client
        return -1;
    }
    //加锁保护发送缓冲区
    pthread_mutex_lock(&sock->send_lock);
    //首先判断缓冲区存在 虽然肯定会在创建时初始化
    if(sock->sending_buf== NULL){
        sock->sending_buf=malloc(MAX_BUF_SIZE);
        if(!sock->sending_buf){
            pthread_mutex_unlock(&sock->send_lock);
            //printf("内存分配失败!");
            return -1;
        }
        sock->sending_len=0;
    }
    //检查缓冲区是否有空间  已发送+将发送是否大于MAX-SIZE
    while(sock->sending_len+len>MAX_BUF_SIZE){
        //如果没有空间 使用忙等待 或者使用条件变量
        pthread_mutex_unlock(&sock->send_lock);//因为不操作了 交给别的part需要解锁
        usleep(1000);
        pthread_mutex_lock(&sock->send_lock);//休眠后 再上锁再次检查
    }
    //正常 将data复制到发送缓冲区
    //printf("【tju_send】复制数据前: sending_len=%d, len=%d, 总长度=%d\n", sock->sending_len, len, sock->sending_len+len);
    memcpy(sock->sending_buf+sock->sending_len,(char*)buffer,len);//从上次发送结束位置开始保存这次的数据
    sock->sending_len+=len;
    //printf("【tju_send】复制数据后: sending_len=%d\n", sock->sending_len);
    //printf("【tju_send】复制数据到缓冲区 - 目标位置偏移=%d, 当前sending_len=%d\n", 
    //       sock->sending_len, sock->sending_len);
    pthread_mutex_unlock(&sock->send_lock);
    //创建发送线程
    if(!send_thread_created){
        send_thread_created=1;
        pthread_t send_thread;
        pthread_create(&send_thread,NULL,send_pkt_thread,sock);
    }
    if(!resend_thread_created){
        resend_thread_created=1;
        pthread_t resend_thread;
        pthread_create(&resend_thread,NULL,resend_pkt_thread,sock);
    }
    return len;//成功发送了len
}
void* send_pkt_thread(void* arg){
    tju_tcp_t* sock =(tju_tcp_t*)arg;//得到socket
    //printf("发送线程启动\n");
    /*发送数据*/
    while(1){
        //缓冲区中有数据要发送&&发送窗口没满可以发送(nextseq-base<win_size)
        if((sock->sending_len>0)&&((sock->window.wnd_send->nextseq-sock->window.wnd_send->base)<sock->window.wnd_send->window_size)){
            pthread_mutex_lock(&sock->send_lock);
            /*实际发送数据*/
            //这里要分段发送 避免单个pkt过大 标准就是MAX_DLEN 最大包内数据长度
            int dlen;
            uint16_t plen;
            //计算可发送数据长度
            uint32_t available_wnd=sock->window.wnd_send->window_size-sock->window.wnd_send->nextseq+sock->window.wnd_send->base;
            //uint32_t remaining_data=sock->sending_len-(sock->window.wnd_send->nextseq-1);
            uint32_t remaining_data=sock->sending_len-(sock->window.wnd_send->nextseq-sock->window.wnd_send->base);
            //sock->window.wnd_send->nextseq-sock->window.wnd_send->base 是已经发送但未确认的部分 也就是还要发送的部分 
            //已经发送并确认的会通过cleaned_len在sending_len中去除
            //可发送数据量 min[剩余未发送数据量,available_wnd] base、nextseq再接收到ack后更新过 这是新的一次发送
            uint32_t data_send_len=MIN(available_wnd,remaining_data);
            //提取要发送的数据 注意这里的计算要特别注意
            //char* data=sock->sending_buf+sock->window.wnd_send->nextseq-sock->send_cleaned_len-1;
            char* data=sock->sending_buf+sock->window.wnd_send->nextseq-sock->window.wnd_send->base;
            //这个sending_buf每次都会重新malloc
            //添加调试日志
             //printf("DEBUG: sending_len=%d, nextseq=%d, base=%d, cleaned_len=%d\n",
                     //sock->sending_len,sock->window.wnd_send->nextseq,
                     //sock->window.wnd_send->base,sock->send_cleaned_len);
             //printf("DEBUG: available_wnd=%d, total_unsent_data=%d, data_send_len=%d\n", 
                //available_wnd, remaining_data, data_send_len);
            if(remaining_data==0){
                pthread_mutex_unlock(&sock->send_lock);
                usleep(10000);//等待10ms再检查
            }else{
            //无需分包 可一次发完
            if(data_send_len<=MAX_DLEN){
                dlen=data_send_len;
                plen=dlen+DEFAULT_HEADER_LEN;
                uint32_t seq=sock->window.wnd_send->nextseq;//这个是可以和之前发送过的pkt联系起来的
                uint32_t ack=0;
                char* pkt_whole=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN,plen,NO_FLAG,1,0,data,dlen);
                sendToLayer3(pkt_whole,plen);
                trace_send(pkt_whole);
                //printf("整包发送:发送数据seq=%d,dlen=%d\n",seq,dlen);
                data_send_len=0;//发送完后置0
                //还要注意这里要把包加入缓冲区
                cache_pkt(sock,seq,data,dlen);
                // 修复：所有包都需要记录发送时间，不只是base包
                // cache_pkt函数内部已经记录了发送时间，这里不需要重复记录
                if(sock->window.wnd_send->base==seq){
                    sock->window.wnd_send->retransmitted_in_flight=0;//标记无重传
                    //startTimer(sock);
                }
                //成功发送 nextseq需要往前推 这个必须在之后 否则计时器无法正常启动
                sock->window.wnd_send->nextseq+=dlen;
            }//无法一次发送完成 需要分包
            else{
                //可能需要多次分包直到发送完为止
                while (data_send_len){
                    if(data_send_len>MAX_DLEN){
                        dlen=MAX_DLEN;
                    }else{
                        dlen=data_send_len;
                    }
                    plen=dlen+DEFAULT_HEADER_LEN;
                    uint32_t seq=sock->window.wnd_send->nextseq;
                    uint32_t ack=0;
                    char* pkt_part=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN,plen,NO_FLAG,1,0,data,dlen);
                    sendToLayer3(pkt_part,plen);
                    free(pkt_part);
                    //printf("分包发送:发送数据seq=%d,dlen=%d\n",seq,dlen);
                    cache_pkt(sock,seq,data,dlen);
                    //if(sock->window.wnd_send->base==seq) startTimer(sock);
                    data_send_len-=dlen;
                    data=data+dlen;
                    sock->window.wnd_send->nextseq+=dlen;
                }
            }
            pthread_mutex_unlock(&sock->send_lock);
            }
            }else{
                //没有数据要发送/窗口空间不足
                pthread_mutex_unlock(&sock->send_lock);
                usleep(10000);//等待10ms再检查
            }
        }
        return NULL;
}
int tju_recv(tju_tcp_t* sock, void *buffer, int len){
    //printf("【tju_recv】开始接收数据，请求长度=%d\n", len);

    while(sock->received_len<=0){
        // 阻塞等待数据
        usleep(1000); // 休眠1ms避免忙等待
    } 

    while(pthread_mutex_lock(&(sock->recv_lock)) != 0); // 加锁

    int read_len = 0;
    if (sock->received_len >= len){ // 从中读取len长度的数据
        read_len = len;
    }else{
        read_len = sock->received_len; // 读取sock->received_len长度的数据(全读出来)
    }
    //printf("Server: 准备调用tju_recv接收数据...\n");
    memcpy(buffer, sock->received_buf, read_len);
    //printf("Server: 实际接收到数据长度=%d\n", read_len);
    
    // 恢复接收窗口大小
    sock->window.wnd_recv->avail_wnd_size += read_len;
    // 确保不超过最大窗口大小
    if(sock->window.wnd_recv->avail_wnd_size > TCP_RECVWN_SIZE){
        sock->window.wnd_recv->avail_wnd_size = TCP_RECVWN_SIZE;
    }
    trace_rwnd(sock->window.wnd_recv->avail_wnd_size / 1375);
    //printf("【tju_recv】恢复接收窗口: 读取%d字节, 当前avail_wnd_size=%d\n", 
           //read_len, sock->window.wnd_recv->avail_wnd_size);
    
    if(read_len < sock->received_len) { // 还剩下一些
        char* new_buf = malloc(sock->received_len - read_len);
        memcpy(new_buf, sock->received_buf + read_len, sock->received_len - read_len);
        free(sock->received_buf);
        sock->received_len -= read_len;
        sock->received_buf = new_buf;
    }else{
        free(sock->received_buf);
        sock->received_buf = NULL;
        sock->received_len = 0;
    }
    pthread_mutex_unlock(&(sock->recv_lock)); // 解锁

    return read_len;
}

int tju_handle_packet(tju_tcp_t* sock, char* pkt){
    uint8_t flags=get_flags(pkt);//得到header中的标志位
    trace_recv(pkt);
    if(sock->state==LISTEN){
        /*第二次握手*/
        if(flags&SYN_FLAG_MASK){
            handle_syn(sock,pkt);//这里的sock是listen socks 
            //这里sock仍然时LISTEN 因为改变状态的是新创建socket
        }
        /*已完成三次握手 服务器最后接收ACK*/
        else if(flags&ACK_FLAG_MASK){
            //sock仍然是LISTEN 会有一个新创建的socket作为服务器响应的sock
            //将半连接队列中的socket转入全连接队列
            uint16_t remote_port = get_src(pkt);  // 客户端端口
            uint16_t local_port = get_dst(pkt);   // 服务端端口
            uint32_t remote_ip = inet_network(CLIENT_IP);//客户端
            uint32_t local_ip = sock->bind_addr.ip;//sock通道是一直不变的 只是pkt会变
            int hashval = cal_hash(local_ip, local_port, remote_ip, remote_port);
            tju_tcp_t* synsock = synqueue[hashval];//把之前放进去的新创建socket找出来
            if(synsock==NULL){
                //printf("未找到对应半连接!");
                return -1;
            }
            //将该socket状态改为ESTABLISHED
            synsock->state=ESTABLISHED;
            //printf("建立连接！服务器socket状态: %d\n", synsock->state);
            //移动到全连接表
            acceptqueue[hashval] = synsock;
            synqueue[hashval] = NULL;
            established_socks[hashval]=synsock;
            //printf("socket已放入acceptqueue，hashval=%d\n", hashval);
            
        }
    }
    /*第三次握手 client*/
    else if(sock->state==SYN_SENT&&(flags == (SYN_FLAG_MASK | ACK_FLAG_MASK)) ){
        uint32_t seq=get_ack(pkt);//seq=y
        uint32_t ack=get_seq(pkt)+1;//ack=x+1
        sock->state=ESTABLISHED;
        char* ack_flags=create_packet_buf(get_dst(pkt),get_src(pkt),seq,ack,DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, ACK_FLAG_MASK, 1, 0, NULL, 0);
        sendToLayer3(ack_flags,DEFAULT_HEADER_LEN);
        //printf("建立连接成功！\n");
        
    }
    /*连接关闭 四次挥手*/
     /*第二次挥手*/
    else if(sock->state==ESTABLISHED){
        //printf("服务器处理数据包，当前状态: %d, flags: %d\n", sock->state, flags);
        if (flags==NO_FLAG){
            //printf("接收到pkt,seq=%d\n",get_seq(pkt));
            
            //首先判断收到数据是否为expectseq
            if(get_seq(pkt)==sock->window.wnd_recv->expect_seq){
                //printf("【有序接收】收到期望包: seq=%u, expect_seq=%u\n", 
                       //get_seq(pkt), sock->window.wnd_recv->expect_seq);
                uint16_t dlen=get_plen(pkt)-get_hlen(pkt);
                uint32_t expt_seq=sock->window.wnd_recv->expect_seq;
                uint32_t avail_wnd_size=sock->window.wnd_recv->avail_wnd_size;//流量控制相关
                //看能接收的数据能不能放入接收缓冲区 否则发送ACK
                if(get_seq(pkt)+dlen<expt_seq+avail_wnd_size){
                    //正常放入
                    pthread_mutex_lock(&sock->recv_lock);
                    //初始化接收缓冲区
                    if(sock->received_buf == NULL){
                        sock->received_buf = malloc(MAX_BUF_SIZE);
                        sock->received_len = 0;
                    }
                    //直接追加到接收缓冲区末尾
                    memcpy(sock->received_buf + sock->received_len, pkt + get_hlen(pkt), dlen);
                    //printf("【有序接收】成功接收数据: seq=%u, dlen=%d, 更新expect_seq %u->%u\n", 
                           //get_seq(pkt), dlen, sock->window.wnd_recv->expect_seq, sock->window.wnd_recv->expect_seq+dlen);
                    //成功接收数据 buf以及窗口等数据更新
                    sock->window.wnd_recv->avail_wnd_size-=dlen;//哪个地方恢复呢
                    sock->window.wnd_recv->expect_seq+=dlen;
                    sock->received_len+=dlen;
                    sock->window.wnd_send->nextseq=get_ack(pkt);//更新发送部分
                    pthread_mutex_unlock(&sock->recv_lock);
                    trace_delv(get_seq(pkt), dlen);
                    trace_rwnd(sock->window.wnd_recv->avail_wnd_size / 1375);
                } else {
                    //printf("【有序接收】窗口空间不足，跳过包: seq=%u\n", get_seq(pkt));
                }
                //处理完之后还要检查是否有缓存pkt可以进行处理
                //printf("【有序接收】处理完有序包，检查缓存包\n");
                check_cached_pkt(sock);
                //发送ACK确认
                uint32_t seq=sock->window.wnd_send->nextseq;
                uint32_t ack=sock->window.wnd_recv->expect_seq;
                uint32_t adv_wnd=sock->window.wnd_recv->avail_wnd_size;
                char* ack_pkt=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,ACK_FLAG_MASK,adv_wnd,0,NULL,0);
                sendToLayer3(ack_pkt,DEFAULT_HEADER_LEN);
                free(ack_pkt);
            }
            /*SR相关处理*/
            //收到seq乱序但仍在接收窗口内 不直接丢弃 而是缓存乱序pkt
            else if(get_seq(pkt)>sock->window.wnd_recv->expect_seq&&get_seq(pkt)<=sock->window.wnd_recv->expect_seq+TCP_RECVWN_SIZE){
                    //printf("【乱序缓存】检测到乱序包: seq=%u, expect_seq=%u, 当前缓存数量=%d\n", 
                           //get_seq(pkt), sock->window.wnd_recv->expect_seq, sock->unolen);
                    //发送ACK
                    uint32_t seq=sock->window.wnd_send->nextseq;
                    uint32_t ack=sock->window.wnd_recv->expect_seq;
                    uint8_t flags=ACK_FLAG_MASK;
                    char* ack_pkt=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,flags,TCP_RECVWN_SIZE-(sock->unolen)*MAX_DLEN,0,NULL,0);
                    sendToLayer3(ack_pkt,DEFAULT_HEADER_LEN);
                    //缓存乱序pkt
                    if(sock->unolen>MAX_PKT_IN_WND) {
                        //printf("【乱序缓存】警告: 缓存包数量超出限制 %d > %d\n", sock->unolen, MAX_PKT_IN_WND);
                        return 0;//超出限制
                    }
                    uint16_t pkt_len = get_plen(pkt);
                    uint16_t hlen = get_hlen(pkt);
                    uint16_t dlen = pkt_len - hlen;
                    //printf("【乱序缓存】包长度信息: plen=%d, hlen=%d, dlen=%d\n", 
                           //pkt_len, hlen, dlen);
                    //printf("【乱序缓存】原始包数据: ");
                    //for(int i = 0; i < 20; i++) {
                    //    printf("%02x ", (unsigned char)pkt[i]);
                    //}
                    //printf("\n");
                    memcpy(sock->unorder[sock->unolen],pkt,pkt_len);//缓存
                    sock->unolen++;
                    //printf("【乱序缓存】成功缓存乱序包: seq=%u, 缓存后数量=%d\n", get_seq(pkt), sock->unolen);
            }else{
                //序列号不在接收窗口内 直接发送ack响应
                //printf("【窗口外】收到窗口外包: seq=%u, expect_seq=%u, 窗口大小=%u\n", 
                       //get_seq(pkt), sock->window.wnd_recv->expect_seq, TCP_RECVWN_SIZE);
                uint32_t seq=sock->window.wnd_send->nextseq;
                uint32_t ack=sock->window.wnd_recv->expect_seq;
                uint8_t flags=ACK_FLAG_MASK;
                uint32_t adv_wnd=TCP_RECVWN_SIZE-(sock->unolen)*MAX_DLEN;
                char* ack_pkt=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,flags,TCP_RECVWN_SIZE-(sock->unolen)*MAX_DLEN,0,NULL,0);
                sendToLayer3(ack_pkt,DEFAULT_HEADER_LEN);
            }
        }
        /*接收ACK报文 也是SR机制重点部分*/
        else if(flags==ACK_FLAG_MASK){
            sender_window_t* send_win=sock->window.wnd_send;
            rtt_stats_t* rtt = &sock->rtt_stats;
            //收到ACK在窗口外 丢弃
            if(get_ack(pkt)<sock->window.wnd_send->base){
                //printf("收到ACK报文在接收窗口外 丢弃\n");
                //return;
            }            //收到重复ack 等于当前base
            else if(get_ack(pkt)==sock->window.wnd_send->base){
                //printf("【快速重传】收到重复ACK: ack=%u, base=%u, 当前计数=%d\n", 
                       //get_ack(pkt), sock->window.wnd_send->base, sock->window.wnd_send->same_ack_cnt);
                //快速重传 ack-cnt=3
                sock->window.wnd_send->same_ack_cnt++;
                //printf("【快速重传】重复ACK计数增加到: %d (目标: 3)\n", sock->window.wnd_send->same_ack_cnt);
                if(sock->window.wnd_send->same_ack_cnt==3){
                    //printf("【快速重传】触发快速重传: seq=%u, base=%u\n", 
                           //sock->window.wnd_send->base, sock->window.wnd_send->base);
                    sock->fast_retransmit_seq=sock->window.wnd_send->base;//设置要重传的seq
                    sock->fast_retransmit=1;
                    handle_fast_retransmit(sock);//直接调用快速重传
                    //这里直接调用快速重传函func 或 等待重传线程判断 均可
                    sock->window.wnd_send->same_ack_cnt=0;//重新置零
                    //printf("【快速重传】快速重传完成，重置计数\n");
                    trace_cwnd(2, sock->window.wnd_send->window_size / 1375); // type=2表示fast retransmit
                }
            }//大于base可以更新
            else{
                //printf("【ACK处理】收到有效ACK: ack=%u, 当前base=%u, 更新base\n", 
                       //get_ack(pkt), sock->window.wnd_send->base);
                sock->window.wnd_send->base=get_ack(pkt);//直接将base设为当前ack
                sock->window.wnd_send->ack_cnt=sock->window.wnd_send->base;//更新已经确认了的数据
                // 重置重复ACK计数，因为收到了有效ACK
                //printf("【ACK处理】重置前: dup_ack计数=%d\n", sock->window.wnd_send->same_ack_cnt);
                sock->window.wnd_send->same_ack_cnt=0;
                //printf("【ACK处理】更新后: base=%u, ack_cnt=%u, 重置dup_ack计数\n", 
                       //sock->window.wnd_send->base, sock->window.wnd_send->ack_cnt);
                //printf("【ACK处理】base=%d, ack_cnt=%d, cleaned_len=%d, 差值=%d\n", 
                       //sock->window.wnd_send->base, sock->window.wnd_send->ack_cnt, 
                       //sock->send_cleaned_len, sock->window.wnd_send->ack_cnt-sock->send_cleaned_len);
                sock->window.wnd_send->window_size=get_advertised_window(pkt);//流量控制 根据pkt反馈改变发送窗口大小
                trace_swnd(sock->window.wnd_send->window_size / 1375);
                
                //RTT计算 - 必须在包清理之前进行
                sr_packet_t* find_pkt=find_pkt_rtt(send_win,get_ack(pkt));
                if(find_pkt!=NULL){
                    //printf("【RTT计算】找到ACK对应包: seq=%u, retransmit_count=%d, retransmitted_in_flight=%d\n", 
                           //find_pkt->seq_num, find_pkt->retransmit_count, rtt->retransmitted_in_flight);
                    //跳过重传包的RTT计算
                    if(find_pkt->retransmit_count==0&&!rtt->retransmitted_in_flight){
                        //计算Sample_RTT
                        struct timeval nowtime;
                        gettimeofday(&nowtime,NULL);
                        long rtt_sample_us=(nowtime.tv_sec - find_pkt->send_time.tv_sec) * 1000000L + 
                                (nowtime.tv_usec - find_pkt->send_time.tv_usec);
                        //printf("【RTT计算】计算RTT样本: %ldμs\n", rtt_sample_us);
                        //更新RTO
                        update_RTO(&sock->rtt_stats,rtt_sample_us);
                        trace_rtts((double)rtt_sample_us/1000.0, 
                                  (double)sock->rtt_stats.estimated_rtt_ms, 
                                  (double)sock->rtt_stats.dev_rtt_ms, 
                                  (double)sock->rtt_stats.timeout_interval_ms);
                    } else {
                        //printf("【RTT计算】跳过RTT计算: 重传包或重传包在飞行中\n");
                    }
                    rtt->retransmitted_in_flight=0;
                } else {
                    //printf("【RTT计算】未找到ACK对应包: ack=%u\n", get_ack(pkt));
                }
                
                //这里累积确认之后 需要立即标记已经确认的包 小于当前ack的都是已经确认过的
                pthread_mutex_lock(&send_win->mutex);
                int packets_acked=0,packets_freed=0;
                for(int i=0;i<MAX_WND_SIZE;i++){
                    sr_packet_t* packet=send_win->packets[i];
                    if(packet!=NULL&&!packet->acked&&(packet->seq_num<get_ack(pkt))){
                        //printf("【ACK处理】确认并清理包: seq=%u, data_len=%zu\n", 
                               //packet->seq_num, packet->data_len);
                        packet->acked=TRUE;
                        packets_acked++;
                        //立即释放内存
                        if(packet->data!=NULL){
                            free(packet->data);
                            packet->data=NULL;
                        }
                        free(packet);
                        send_win->packets[i]=NULL;
                        packets_freed++;
                    }
                }
                //printf("【ACK处理】包确认统计: 确认包数=%d, 释放包数=%d\n", packets_acked, packets_freed);
                pthread_mutex_unlock(&send_win->mutex);
                
                //清理发送缓冲区 - 在RTT计算之后执行
                //printf("【清理检查】ack_cnt=%d, cleaned_len=%d, 差值=%d\n", 
                       //sock->window.wnd_send->ack_cnt, sock->send_cleaned_len, 
                       //sock->window.wnd_send->ack_cnt-sock->send_cleaned_len);
                if(sock->window.wnd_send->ack_cnt-sock->send_cleaned_len>0){
                    pthread_mutex_lock(&sock->send_lock);
                    //printf("【清理缓冲区】清理前: sending_len=%d, ack_cnt=%d, cleaned_len=%d\n", 
                           //sock->sending_len, sock->window.wnd_send->ack_cnt, sock->send_cleaned_len);
                    //重新创建一个代替旧的
                    char* new_sending_buf=(char*)malloc(MAX_BUF_SIZE);
                    uint32_t remaining_data_len = sock->sending_len - (sock->window.wnd_send->ack_cnt - sock->send_cleaned_len);
                    //printf("【清理缓冲区】剩余数据长度: %d\n", remaining_data_len);
                    if(remaining_data_len > 0){
                        memcpy(new_sending_buf, sock->sending_buf + sock->window.wnd_send->ack_cnt - sock->send_cleaned_len, remaining_data_len);
                    }
                    free(sock->sending_buf);
                    sock->sending_buf=new_sending_buf;
                    sock->sending_len=remaining_data_len;
                    sock->send_cleaned_len=sock->window.wnd_send->ack_cnt;
                    //printf("【清理缓冲区】清理后: sending_len=%d, cleaned_len=%d\n", sock->sending_len, sock->send_cleaned_len);
                    pthread_mutex_unlock(&sock->send_lock);
                }
            }
        }
        
        if(flags&FIN_FLAG_MASK){
            // server 接受到FIN 发送ACK 进入CLOSE_WAIT
            uint32_t ack=get_seq(pkt)+1;
            uint32_t seq=get_ack(pkt);
            sock->state=CLOSE_WAIT;
            char* ack_flags=create_packet_buf(get_dst(pkt),get_src(pkt),seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,ACK_FLAG_MASK,1,0,NULL,0);
            sendToLayer3(ack_flags,DEFAULT_HEADER_LEN);
            //此时socket处于半关闭状态 server还可以发送数据
            //server需要将剩余的数据处理完 再调用close发送FIN
            sleep(1);// 然后等待1s 发FIN+ACK
            
             /*第三次挥手*/
             //等待完成后直接发送FIN+ACK 注意ack一致
             char* fin_ack_flags=create_packet_buf(get_dst(pkt),get_src(pkt),0,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,(ACK_FLAG_MASK|FIN_FLAG_MASK),1,0,NULL,0);
            sendToLayer3(fin_ack_flags,DEFAULT_HEADER_LEN);
            sock->state=LAST_ACK;
            sock->packet_FIN = fin_ack_flags;
        }
    }
    else if(sock->state==FIN_WAIT_1){
        /*第二次挥手 接收到ACK*/
        /*要注意这里要和同时关闭接收到FIN信号区分！不然会导致无法正确判断
        因为同时关闭接收到的信号是FIN+ACk 要么要求不等于要么加上两者条件提前*/
        //client接收到ACK 进入FIN_WAIT_2 
        if(flags&ACK_FLAG_MASK&&!(flags & FIN_FLAG_MASK)){
        sock->state=FIN_WAIT_2;
        }
        /*client和server同时关闭的情况*/
        else if (flags&FIN_FLAG_MASK)
        {
            //收到FIN 继续发送ACK
            uint32_t ack=get_seq(pkt)+1;
            uint32_t seq=1;//此时
            char* ack_flags=create_packet_buf(get_dst(pkt),get_src(pkt),seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,ACK_FLAG_MASK,1,0,NULL,0);
            sendToLayer3(ack_flags,DEFAULT_HEADER_LEN);
            sock->packet_FIN=ack_flags;
            sock->state=CLOSING;
            Timeout_retransmission(sock, CLOSED, ack_flags, DEFAULT_HEADER_LEN);
        }
        // else if(flags==(FIN_FLAG_MASK|ACK_FLAG_MASK)){
        //     sock->state=CLOSING;
        //     uint32_t ack=get_seq(pkt)+1;
        //     uint32_t seq=get_ack(pkt)+1;
        //     char* ack_flags=create_packet_buf(get_dst(pkt),get_src(pkt),seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,ACK_FLAG_MASK,1,0,NULL,0);
        //     sendToLayer3(ack_flags,DEFAULT_HEADER_LEN);
        //     sock->packet_FIN=ack_flags;
        //     Timeout_retransmission(sock, CLOSED, ack_flags, DEFAULT_HEADER_LEN);
        // }
    }
    /*第四次握手*/
    else if(sock->state==FIN_WAIT_2&&(flags==(FIN_FLAG_MASK|ACK_FLAG_MASK))){
        /*client接收到FIN+ACK 进入TIME_WAIT 并发送最后一个ACK
        并启动2MSL时间定时器 超时后进进入closed 彻底关闭连接*/
        uint32_t seq=get_ack(pkt);
        uint32_t ack=get_seq(pkt)+1;
        char* ack_flags=create_packet_buf(get_dst(pkt),get_src(pkt),seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,ACK_FLAG_MASK,1,0,NULL,0);
        sendToLayer3(ack_flags,DEFAULT_HEADER_LEN);
        sock->state=TIME_WAIT;//只需要等待是否超时即可
        //启动2MSL定时器 状态变为CLOSED 当然也可以直接休息sleep
        timer_2msl(sock);
    }
    /*第四次握手 server接收最后一次ACK*/
    else if(sock->state==LAST_ACK&&(flags&ACK_FLAG_MASK)){
        sock->state=CLOSED;
        //将socket标记为可释放状态
        sock->to_be_free=1;
    }
    /*同时关闭 接收ACK*/
    else if(sock->state==CLOSING){
        if(flags==ACK_FLAG_MASK)//防止随意关闭
        {
            sock->state=TIME_WAIT;
            timer_2msl(sock);
        }
    }
    return 0;
}

int tju_close (tju_tcp_t* sock){
    if(sock->state==ESTABLISHED){
        //发送FIN报文
        //tju_send(sock,NULL,0,FIN_FLAG_MASK);
        //这里的seq从收到的pkt得来
        uint32_t seq=0;
        uint32_t ack=0;
        char* fin_ack_flags=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,FIN_FLAG_MASK|ACK_FLAG_MASK,0,0,NULL,0);
        //将socket状态改为FIN_WAIT_1
        sock->state=FIN_WAIT_1;
        sendToLayer3(fin_ack_flags,DEFAULT_HEADER_LEN);
        sock->packet_FIN = fin_ack_flags;
       // time_point=clock();
    }
    //这里也要支持超时重传 也是简单的忙等待
    //阻塞等待直到状态变CLOSED
    //ps这里先直接用休眠简单充当一下定时
    while(sock->state != CLOSED);
    //关闭 释放资源
    if(sock->to_be_free==1)
    free_socket_resources(sock);
    
    return 0;
}
/*第二次握手*/
void handle_syn(tju_tcp_t* listen_socks,char* pkt){
    tju_tcp_t* synsock = (tju_tcp_t*)malloc(sizeof(tju_tcp_t));
    memcpy(synsock, listen_socks, sizeof(tju_tcp_t));
    //初始化，并设置state为SYN_RECV
    uint32_t remote_ip = inet_network(CLIENT_IP);//由于写定了 故直接写
    synsock->established_local_addr=listen_socks->bind_addr;
    synsock->established_remote_addr.port=get_src(pkt); //确定端口
    synsock->established_remote_addr.ip=remote_ip;//服务器的remote即客户端ip
    synsock->state=SYN_RECV;
    //将新建socket放入半连接队列
    int hashval = cal_hash(
        synsock->established_local_addr.ip,
        synsock->established_local_addr.port,
        synsock->established_remote_addr.ip,
        synsock->established_remote_addr.port
    );
    synqueue[hashval]=synsock;
    //printf("handle_syn: socket放入synqueue，hashval=%d，状态=%d\n", hashval, synsock->state);
    uint32_t seq=0;
    uint32_t ack=get_seq(pkt)+1;//ack=x+1 seq=y 随机设置为0
    char* syn_ack_flags=create_packet_buf(get_dst(pkt),get_src(pkt),seq,ack,DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, (SYN_FLAG_MASK|ACK_FLAG_MASK), 1, 0, NULL, 0);
    sendToLayer3(syn_ack_flags,DEFAULT_HEADER_LEN);
    //这里要考虑进制计算 这两个位置同时置1 00010010
}
/*关闭连接回收资源*/
void free_socket_resources(tju_tcp_t* sock){
    if(sock==NULL)return; //已经为空直接返回
    //释放接收缓存区
    if(sock->received_buf!=NULL){
        free(sock->received_buf);
        sock->received_buf=NULL;
        sock->received_len=1;
    }
    //销毁互斥锁
    //从表中移除 全连接+已连接表
    uint32_t local_ip = sock->established_local_addr.ip;
    uint16_t local_port = sock->established_local_addr.port;
    uint32_t remote_ip = sock->established_remote_addr.ip;
    uint16_t remote_port = sock->established_remote_addr.port;
    
    int hashval = cal_hash(local_ip, local_port, remote_ip, remote_port);
    
    // 从已建立连接表中移除
    if (established_socks[hashval] == sock) {
        established_socks[hashval] = NULL;
    }
    
    // 从accept队列中移除
    for (int i = 0; i < MAX_SOCK; i++) {
        if (acceptqueue[i] == sock) {
            acceptqueue[i] = NULL;
            break;
        }
    }
    //释放socket本身
    free(sock);
}
/*添加定时器机制*/
void timer_2msl(tju_tcp_t* sock){
    //使用2msl定时器 超时后设置状态为CLOSED
    pthread_t timer_thread;
    pthread_create(&timer_thread,NULL,msl_timer_thread,(void*)sock);
}
void* msl_timer_thread(void* arg){
    tju_tcp_t* sock=(tju_tcp_t*)arg;
    usleep(2*MSL*1000);
    sock->state=CLOSED;
    sock->to_be_free=1;
    return NULL;
}

/*用于生成随机端口 1024-65535*/
uint16_t random_port(){
    srand((unsigned int) time(NULL));
    uint16_t port=(rand()%(65535-1024+1))+1024;
    return port;
}
/*超时重传*/
void Timeout_retransmission(tju_tcp_t* sock, int exp_state, char* pkt, int pktlen) { 
    long timeout = 100000L;
    struct timeval start_time, end_time;
    gettimeofday(&start_time, NULL);
    while (sock->state != exp_state){
        gettimeofday(&end_time, NULL);
        long Time = 1000000L * (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_usec - start_time.tv_usec);
        if (Time >= timeout) { //超时重传
            gettimeofday(&start_time, NULL);
            sendToLayer3(pkt, pktlen);
        }
    }
}
/*在将pkt放入接收缓冲区后 确认缓存的乱序pkt是否可以进一步处理*/
void check_cached_pkt(tju_tcp_t* sock){
    //printf("【乱序处理】开始检查缓存包: 缓存数量=%d, expect_seq=%u\n", 
           //sock->unolen, sock->window.wnd_recv->expect_seq);
    //没有缓存乱序pkt 直接发送ACK
    if(sock->unolen==0){
        //printf("【乱序处理】无缓存包，直接发送ACK\n");
        uint32_t seq=sock->window.wnd_send->nextseq;
        uint32_t ack=sock->window.wnd_recv->expect_seq;
        uint8_t flags=ACK_FLAG_MASK;
        char* ack_pkt=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,flags,TCP_RECVWN_SIZE-(sock->unolen)*MAX_DLEN,0,NULL,0);
        sendToLayer3(ack_pkt,DEFAULT_HEADER_LEN);
    }
    else{
        int len=sock->unolen;
        //printf("【乱序处理】开始排序 %d 个缓存包\n", len);
        //先对缓存的乱序pkt按seq进行排序
        for(int i=0;i<len;i++){
            for(int j=i+1;j<len;j++){
                if(get_seq(sock->unorder[i])>get_seq(sock->unorder[j])){
                    //printf("【乱序处理】交换包位置: seq[%d]=%u <-> seq[%d]=%u\n", 
                           //i, get_seq(sock->unorder[i]), j, get_seq(sock->unorder[j]));
                    my_swap(sock->unorder[i], sock->unorder[j]);
                }
            }
        }
        //printf("【乱序处理】排序完成，开始查找可处理包\n");
        //排序完后 逐个遍历看是否有与expectseq相符的
        int index=0;
        for(;index<len;index++){
            //printf("【乱序处理】检查缓存包[%d]: seq=%u, expect_seq=%u\n", 
                   //index, get_seq(sock->unorder[index]), sock->window.wnd_recv->expect_seq);
            if(get_seq(sock->unorder[index])==sock->window.wnd_recv->expect_seq){
                //那么就将这个pkt放入接收缓冲区中
                char* pkt=sock->unorder[index];
                uint16_t pkt_len = get_plen(pkt);
                uint16_t hlen = get_hlen(pkt);
                uint16_t dlen = pkt_len - hlen;
                uint32_t expt_seq=sock->window.wnd_recv->expect_seq;
                uint32_t avail_wnd_size=sock->window.wnd_recv->avail_wnd_size;//流量控制相关
                //printf("【乱序处理】找到匹配包: seq=%u, plen=%d, hlen=%d, dlen=%d, avail_wnd=%u\n", 
                       //get_seq(pkt), pkt_len, hlen, dlen, avail_wnd_size);
                //printf("【乱序处理】缓存包数据: ");
                //for(int i = 0; i < 20; i++) {
                //    printf("%02x ", (unsigned char)pkt[i]);
                //}
                //printf("\n");
                //看能接收的数据能不能放入接收缓冲区 否则发送ACK
                if(get_seq(pkt)+dlen<expt_seq+avail_wnd_size){
                    //正常放入
                    pthread_mutex_lock(&sock->recv_lock);
                    //初始化接收缓冲区
                    if(sock->received_buf == NULL){
                        sock->received_buf = malloc(MAX_BUF_SIZE);
                        sock->received_len = 0;
                    }
                    //直接追加到接收缓冲区末尾 这里recv_len还没有更新 pkt+get_hlen(pkt) 就是直接从数据位置开始
                    memcpy(sock->received_buf + sock->received_len, pkt + get_hlen(pkt), dlen);
                    //printf("【乱序处理】成功处理缓存包: seq=%u, dlen=%d, 更新expect_seq %u->%u\n", 
                           //get_seq(pkt), dlen, sock->window.wnd_recv->expect_seq, sock->window.wnd_recv->expect_seq+dlen);
                    //成功接收数据 buf以及窗口等数据更新
                    sock->window.wnd_recv->avail_wnd_size-=dlen;
                    sock->window.wnd_recv->expect_seq+=dlen;
                    sock->received_len+=dlen;
                    sock->window.wnd_send->nextseq=get_ack(pkt);//更新发送部分
                    pthread_mutex_unlock(&sock->recv_lock);
                } else {
                    //printf("【乱序处理】窗口空间不足，跳过包: seq=%u\n", get_seq(pkt));
                }
            }//当前乱序seq小于expectseq 看后面是否有符合的
            else if(get_seq(sock->unorder[index])<(sock->window.wnd_recv->expect_seq)) {
                //printf("【乱序处理】包seq=%u小于expect_seq=%u，跳过\n", 
                       //get_seq(sock->unorder[index]), sock->window.wnd_recv->expect_seq);
                continue;
            }
            else {
                //printf("【乱序处理】包seq=%u大于expect_seq=%u，停止查找\n", 
                       //get_seq(sock->unorder[index]), sock->window.wnd_recv->expect_seq);
                break;//直到超过仍然没有符合 就跳出
            }
        }
        //部分乱序pkt确认 需要更新乱序缓冲区
        //printf("【乱序处理】处理了 %d 个包，剩余 %d 个缓存包\n", index, len-index);
        for(int i=index;i<len;i++){
            memcpy(sock->unorder[i-index],sock->unorder[i],sizeof(sock->unorder[i]));//len-index后面剩余的部分先无需更新 因为此时unolen会更新
        }
        sock->unolen-=index;
        //printf("【乱序处理】更新缓存数组: 处理前=%d, 处理后=%d\n", len, sock->unolen);
        //全部更新完成后 再次发送ack确认报文
        uint32_t seq=sock->window.wnd_send->nextseq;
        uint32_t ack=sock->window.wnd_recv->expect_seq;
        uint8_t flags=ACK_FLAG_MASK;
        char* ack_pkt=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,flags,TCP_RECVWN_SIZE-(sock->unolen)*MAX_DLEN,0,NULL,0);
        //printf("【乱序处理】发送最终ACK: ack=%u, 剩余缓存=%d\n", ack, sock->unolen);
        sendToLayer3(ack_pkt,DEFAULT_HEADER_LEN);
    }
}
void my_swap(char* a, char* b){
    char temp[MAX_LEN];
    memcpy(temp, a, MAX_LEN);
    memcpy(a, b, MAX_LEN);
    memcpy(b, temp, MAX_LEN);
}

/*重传线程*/
//在受限的SR机制中 只重传
void* resend_pkt_thread(void* arg){
    //线程一直运行 每过固定时间检查每个未确认包 是否超时
    tju_tcp_t* sock=(tju_tcp_t*)arg;
    sender_window_t* send_win=sock->window.wnd_send;
    rtt_stats_t* rtt=&sock->rtt_stats;
    //printf("SR重传线程启动 初始重传间隔为=%ldms\n",rtt->timeout_interval_ms);
    while(sock->state==ESTABLISHED||sock->state==CLOSE_WAIT){
        //检查是否需要快速重传 （高优先级）
        // if(sock->fast_retransmit){
        //     handle_fast_retransmit(sock);
        //     sock->fast_retransmit=0;//重置
        // }
        //检查所有未确认包是否超时 （低优先级）
        check_pkt_timeout(sock);
        //间隔时间检查 - 根据RTO动态调整检查间隔
        long check_interval_us = rtt->timeout_interval_ms * 1000 / 4; // 检查间隔 = RTO/4
        // 设置最小和最大检查间隔
        if(check_interval_us < 10000) check_interval_us = 10000;    // 最小10ms
        if(check_interval_us > 200000) check_interval_us = 200000;  // 最大200ms
        // 只在RTO变化时输出调试信息
        static long last_rto_ms = 0;
        if(rtt->timeout_interval_ms != last_rto_ms) {
            //printf("【重传线程】RTO变化: %ldms -> %ldms, 检查间隔=%ldμs\n", 
                   //last_rto_ms, rtt->timeout_interval_ms, check_interval_us);
            last_rto_ms = rtt->timeout_interval_ms;
        }
        usleep(check_interval_us);
    }
    //printf("退出重传线程\n");
    return NULL;
}
void check_pkt_timeout(tju_tcp_t* sock){
    sender_window_t* send_win=sock->window.wnd_send;
    rtt_stats_t* rtt=&sock->rtt_stats;
    //检查对应包是否超时
    struct timeval nowtime;
    gettimeofday(&nowtime,NULL);
    int timeout_count=0;//不能无限重传
    int unacked_packets=0;
    for(int i=0;i<MAX_WND_SIZE;i++){
        sr_packet_t* packet=send_win->packets[i];
        if(packet!=NULL&&!packet->acked){
            unacked_packets++;
            //计算传输用时
            long elapsed_us=(nowtime.tv_sec - packet->send_time.tv_sec) * 1000000L + 
                     (nowtime.tv_usec - packet->send_time.tv_usec);
            //指数退避判断超时 - 修复单位不一致问题
            long current_timeout_us=rtt->timeout_interval_ms*1000*(1L<<packet->retransmit_count);
            //限制最大超时时间 (60秒)
            if(current_timeout_us>60000000) current_timeout_us=60000000;
            
            // 调试输出：超时检查信息
            //if(elapsed_us > current_timeout_us * 0.8) { // 接近超时时输出
            //    printf("【超时检查】包seq=%u: 已用时=%ldμs, 超时阈值=%ldμs, 重传次数=%d\n", 
            //           packet->seq_num, elapsed_us, current_timeout_us, packet->retransmit_count);
            //}
            
            //判断是否超时
            if(elapsed_us>current_timeout_us){//超时
                //重传 附上当前时间
                handle_timeout_pkt(sock,packet,nowtime);
            }
        }
    }
    // 输出未确认包统计信息
    //if(unacked_packets > 0) {
    //    printf("【超时检查】未确认包数量: %d, base=%u, nextseq=%u, RTO=%ldms\n", 
    //           unacked_packets, send_win->base, send_win->nextseq, rtt->timeout_interval_ms);
    //}
}

/*缓存发送的pkt*/
int cache_pkt(tju_tcp_t* sock,uint32_t seq_num,char* data,int data_len){
    sender_window_t* send_win=sock->window.wnd_send;
    //创建包结构
    sr_packet_t* packet=malloc(sizeof(sr_packet_t));
    if(!packet) return 0;
    //复制数据
    packet->data=malloc(data_len);
    if(!packet->data){
        free(packet);
        return 0;
    }
    memcpy(packet->data,data,data_len);
    //设置包信息
    packet->data_len=data_len;
    packet->seq_num=seq_num;
    gettimeofday(&packet->send_time,NULL);
    packet->retransmit_count=0;
    packet->acked=0;
    //缓存到缓冲区中 - 寻找空闲槽位
    uint32_t slot = seq_num % MAX_WND_SIZE;
    // 如果槽位被占用，寻找下一个空闲槽位
    while(send_win->packets[slot] != NULL && send_win->packets[slot]->seq_num != seq_num) {
        slot = (slot + 1) % MAX_WND_SIZE;
    }
    send_win->packets[slot]=packet;
    //printf("【包缓存】缓存发送包: seq=%u, data_len=%d, slot=%u\n", 
           //seq_num, data_len, slot);
    //发送包行为已经在线程中完成了
    return 1;
}
sr_packet_t* find_pkt_rtt(sender_window_t* send_win,uint32_t ack_num){
    for(int i=0;i<MAX_WND_SIZE;i++){
        sr_packet_t* packet=send_win->packets[i];
        if(packet!=NULL&&packet->data!=NULL&&packet->seq_num+packet->data_len==ack_num){
            //修复packet可能为空指针的问题
            return packet;
        }
    }
    return NULL;
}
void update_RTO(rtt_stats_t* rtt,long samplertt_us){
    /*RTO计算*/
    // 首次计算RTO
    if (rtt->rtt_initialized == 0) {
        //printf("首次RTT测量: R1 = %ldμs\n", samplertt_us);
        // SRTT smooth RTT = samplertt = R1
        rtt->estmated_rtt = samplertt_us;
        // DevRTT = R1/2
        rtt->dev_rtt = samplertt_us / 2;
        rtt->rtt_initialized = 1; // 已经完成计算
    }
    /*后续RTT测量*/
    else {
        long prev_srtt = rtt->estmated_rtt;
        long prev_devrtt = rtt->dev_rtt;
        
        //printf("后续RTT测量: 样本=%ldμs, 前SRTT=%ldμs, 前DevRTT=%ldμs\n", 
               //samplertt_us, prev_srtt, prev_devrtt);
        
        // α = 0.125 代入得 SRTT = (7/8) * SRTT + (1/8) * RTT
        rtt->estmated_rtt = (7 * prev_srtt + samplertt_us) / 8;
        
        // β = 1/4 代入得 DevRTT = (3/4) * DevRTT + (1/4) * |RTT - SRTT|
        long rtt_diff = (samplertt_us > prev_srtt) ? 
                       (samplertt_us - prev_srtt) : (prev_srtt - samplertt_us);
        rtt->dev_rtt = (3 * prev_devrtt + rtt_diff) / 4;
        
        //printf("RTT更新完成: SRTT=%ldμs, DevRTT=%ldμs, |RTT-SRTT|=%ldμs\n",
               //rtt->estmated_rtt, rtt->dev_rtt, rtt_diff);
    }
    
    /*RTO计算部分*/  
    // μ = 1, ∂ = 4，代入得 RTO = SRTT + 4 * DevRTT              
    long rto_us = rtt->estmated_rtt + 4 * rtt->dev_rtt;
    
    // 对RTO进行检查 有最大最小值
    if (rto_us < 100000) {
        //printf("RTO边界检查: %ldμs < 100ms, 调整为100ms\n", rto_us);
        rto_us = 100000;  // 最小RTO = 100ms
    }
    if (rto_us > 60000000) {
        //printf("RTO边界检查: %ldμs > 60s, 调整为60s\n", rto_us);
        rto_us = 60000000;  // 最大RTO = 60s
    }
    
    // 更新timeval结构
    rtt->timeout.tv_sec = rto_us / 1000000L;
    rtt->timeout.tv_usec = rto_us % 1000000L;
    
    // 更新毫秒单位（用于显示和检查）
    rtt->estimated_rtt_ms = rtt->estmated_rtt / 1000;
    rtt->dev_rtt_ms = rtt->dev_rtt / 1000;
    rtt->timeout_interval_ms = rto_us / 1000;
    rtt->rto_ms = rtt->timeout_interval_ms;
    //printf("RTO更新完成: RTO=%ldms\n", rtt->timeout_interval_ms);
}
/*重传数据包*/
void handle_timeout_pkt(tju_tcp_t* sock,sr_packet_t* packet,struct timeval nowtime){
    //printf("【超时重传】开始处理超时重传: seq=%u, retransmit_count=%d\n", 
           //packet->seq_num, packet->retransmit_count);
    sender_window_t* send_win=sock->window.wnd_send;
    rtt_stats_t* rtt=&sock->rtt_stats;
    //检查最大重传次数 MAX=4
    if(packet->retransmit_count>=4){
        //printf("【超时重传】失败: 达到最大重传次数4次, seq=%u\n", packet->seq_num);
        sock->state=CLOSED;
        return;
    }
    //超时后加倍RTO 指数退避 - 修复RTO更新逻辑
    long old_rto = rtt->timeout_interval_ms;
    rtt->timeout_interval_ms = MIN(rtt->timeout_interval_ms * 2, 60000);
    rtt->retransmitted_in_flight = 1;
    //printf("【超时重传】RTO更新: %ldms -> %ldms (指数退避)\n", old_rto, rtt->timeout_interval_ms);
    trace_cwnd(3, sock->window.wnd_send->window_size / 1375); // type=3表示timeout
    //重传pkt
    char* resend_pkt=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,packet->seq_num,0,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN+packet->data_len,NO_FLAG,1,0,packet->data,packet->data_len);
    //printf("【超时重传】发送重传包: seq=%u, data_len=%zu, total_len=%zu\n", 
           //packet->seq_num, packet->data_len, packet->data_len+DEFAULT_HEADER_LEN);
    sendToLayer3(resend_pkt,packet->data_len+DEFAULT_HEADER_LEN);
    trace_send(resend_pkt);
    free(resend_pkt);
    //更新状态
    packet->retransmit_count++;
    packet->send_time=nowtime;
    //printf("【超时重传】重传完成: seq=%u, 重传次数=%d\n", 
           //packet->seq_num, packet->retransmit_count);
}
void handle_fast_retransmit(tju_tcp_t* sock){
    //printf("【快速重传】开始处理快速重传: seq=%u\n", sock->fast_retransmit_seq);
    sender_window_t* send_win=sock->window.wnd_send;
    pthread_mutex_lock(&send_win->mutex);
    uint32_t retransmit_seq=sock->fast_retransmit_seq;//得到要重传包的seq
    //找到要重传的包
    sr_packet_t* packet=NULL;
    //printf("【快速重传】查找包: retransmit_seq=%u\n", retransmit_seq);
    for(int i=0;i<MAX_WND_SIZE;i++){
        if(send_win->packets[i]!=NULL){
            //printf("【快速重传】检查包[%d]: seq=%u, acked=%d\n", 
                   //i, send_win->packets[i]->seq_num, send_win->packets[i]->acked);
            if(send_win->packets[i]->seq_num==retransmit_seq && !send_win->packets[i]->acked){
                packet=send_win->packets[i];
                //printf("【快速重传】找到要重传的包: seq=%u, data_len=%zu, retransmit_count=%d\n", 
                       //packet->seq_num, packet->data_len, packet->retransmit_count);
                break;
            }
        }
    }
    if(packet==NULL){
        //printf("【快速重传】失败: 找不到seq=%u的包\n", retransmit_seq);
        pthread_mutex_unlock(&send_win->mutex);
        return;
    }
    //执行快速重传
    char* fast_retrans_pkt=create_packet_buf(
        sock->established_local_addr.port,
        sock->established_remote_addr.port,
        packet->seq_num,0,
        DEFAULT_HEADER_LEN,
        packet->data_len+DEFAULT_HEADER_LEN,
        NO_FLAG,1,0,
        packet->data,packet->data_len
    );
    if(fast_retrans_pkt!=NULL){
        //printf("【快速重传】发送重传包: seq=%u, data_len=%zu, total_len=%zu\n", 
               //packet->seq_num, packet->data_len, DEFAULT_HEADER_LEN+packet->data_len);
        sendToLayer3(fast_retrans_pkt,DEFAULT_HEADER_LEN+packet->data_len);
        trace_send(fast_retrans_pkt);
        free(fast_retrans_pkt);
        //更新包状态
        packet->retransmit_count++;
        gettimeofday(&packet->send_time,NULL);//重置发送时间
        //标记有重传包在运输中 避免RTT计算误差
        sock->rtt_stats.retransmitted_in_flight=1;
        //printf("【快速重传】重传完成: seq=%u, 重传次数=%d\n", 
               //packet->seq_num, packet->retransmit_count);
    }else{
        //printf("【快速重传】失败: 包构造失败\n");
    }
    pthread_mutex_unlock(&send_win->mutex);
}