#include "tju_tcp.h"
static int send_thread_created = 0;
int resend_thread_created=0;
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
            printf("内存分配失败!");
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
    printf("【tju_send】复制数据前: sending_len=%d, len=%d, 总长度=%d\n", sock->sending_len, len, sock->sending_len+len);
    memcpy(sock->sending_buf+sock->sending_len,(char*)buffer,len);//从上次发送结束位置开始保存这次的数据
    sock->sending_len+=len;
    printf("【tju_send】复制数据后: sending_len=%d\n", sock->sending_len);
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
    printf("发送线程启动\n");
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
             printf("DEBUG: sending_len=%d, nextseq=%d, base=%d, cleaned_len=%d\n",
                     sock->sending_len,sock->window.wnd_send->nextseq,
                     sock->window.wnd_send->base,sock->send_cleaned_len);
             printf("DEBUG: available_wnd=%d, total_unsent_data=%d, data_send_len=%d\n", 
                available_wnd, remaining_data, data_send_len);
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
                printf("整包发送:发送数据seq=%d,dlen=%d\n",seq,dlen);
                data_send_len=0;//发送完后置0
                //还要注意这里要把包加入缓冲区
                cache_pkt(sock,seq,data,dlen);
                //这里应该要启动计时器 超时重传机制
                if(sock->window.wnd_send->base==seq){
                    gettimeofday(&sock->window.wnd_send->send_time,NULL); //记录发送时间
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
                    printf("分包发送:发送数据seq=%d,dlen=%d\n",seq,dlen);
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
    printf("Server: 准备调用tju_recv接收数据...\n");
    memcpy(buffer, sock->received_buf, read_len);
    printf("Server: 实际接收到数据长度=%d\n", read_len);
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
                printf("未找到对应半连接!");
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
        printf("建立连接成功！\n");
    }
    /*连接关闭 四次挥手*/
     /*第二次挥手*/
    else if(sock->state==ESTABLISHED){
        printf("服务器处理数据包，当前状态: %d, flags: %d\n", sock->state, flags);
        if (flags==NO_FLAG){
            printf("接收到pkt,seq=%d\n",get_seq(pkt));
            //首先判断收到数据是否为expectseq
            if(get_seq(pkt)==sock->window.wnd_recv->expect_seq){
                printf("收到数据 seq=%d\n",get_seq(pkt));
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
                    printf("在接收缓冲区末尾放入大小为dlen=%d的数据\n",dlen);
                    //成功接收数据 buf以及窗口等数据更新
                    sock->window.wnd_recv->avail_wnd_size-=dlen;
                    sock->window.wnd_recv->expect_seq+=dlen;
                    sock->received_len+=dlen;
                    sock->window.wnd_send->nextseq=get_ack(pkt);//更新发送部分
                    pthread_mutex_unlock(&sock->recv_lock);
                }
                //处理完之后还要检查是否有缓存pkt可以进行处理
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
                    //发送ACK
                    uint32_t seq=sock->window.wnd_send->nextseq;
                    uint32_t ack=sock->window.wnd_recv->expect_seq;
                    uint8_t flags=ACK_FLAG_MASK;
                    char* ack_pkt=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,flags,TCP_RECVWN_SIZE-(sock->unolen)*MAX_DLEN,0,NULL,0);
                    sendToLayer3(ack_pkt,DEFAULT_HEADER_LEN);
                    //缓存乱序pkt
                    if(sock->unolen>MAX_PKT_IN_WND) return 0;//超出限制
                    memcpy(sock->unorder[sock->unolen],pkt,get_plen(pkt));//缓存
                    sock->unolen++;
            }else{
                //序列号不再接收窗口内 直接发送ack响应
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
                printf("收到ACK报文在接收窗口外 丢弃\n");
                //return;
            }//收到重复ack 等于当前base
            else if(get_ack(pkt)==sock->window.wnd_send->base){
                printf("收到重复ACK报文 ACK=%d\n",get_ack(pkt));
                //快速重传 ack-cnt=3
                sock->window.wnd_send->same_ack_cnt++;
                if(sock->window.wnd_send->same_ack_cnt==3){
                    sock->fast_retransmit=1;
                    sock->fast_retransmit_seq=send_win->base;//这里需要设置快速重传seq
                    //这里直接调用快速重传函func 或 等待重传线程判断 均可
                    sock->window.wnd_send->same_ack_cnt=0;//重新置零
                }
            }//大于base可以更新
            else{
                printf("收到有效ACK报文 ACK=%d\n",get_ack(pkt));
                sock->window.wnd_send->base=get_ack(pkt);//直接将base设为当前ack
                sock->window.wnd_send->ack_cnt=sock->window.wnd_send->base;//更新已经确认了的数据
                printf("【ACK处理】base=%d, ack_cnt=%d, cleaned_len=%d, 差值=%d\n", 
                       sock->window.wnd_send->base, sock->window.wnd_send->ack_cnt, 
                       sock->send_cleaned_len, sock->window.wnd_send->ack_cnt-sock->send_cleaned_len);
                sock->window.wnd_send->window_size=get_advertised_window(pkt);//流量控制 根据pkt反馈改变发送窗口大小
                //这里累积确认之后 需要立即标记已经确认的包 小于当前ack的都是已经确认过的
                pthread_mutex_lock(&send_win->mutex);
                int packets_acked=0,packets_freed=0;
                for(int i=0;i<MAX_WND_SIZE;i++){
                    sr_packet_t* packet=send_win->packets[i];
                    if(packet!=NULL&&!packet->acked&&(packet->seq_num<get_ack(pkt))){
                        //printf("确认并清理包：seq=%d\n",packet->seq_num);
                        packet->acked=TRUE;
                        printf("确认seq=%d\n",packet->seq_num);
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
                pthread_mutex_unlock(&send_win->mutex);
                
                //清理发送缓冲区 - 移到RTT计算之前，确保总是执行
                printf("【清理检查】ack_cnt=%d, cleaned_len=%d, 差值=%d\n", 
                       sock->window.wnd_send->ack_cnt, sock->send_cleaned_len, 
                       sock->window.wnd_send->ack_cnt-sock->send_cleaned_len);
                if(sock->window.wnd_send->ack_cnt-sock->send_cleaned_len>0){
                    pthread_mutex_lock(&sock->send_lock);
                    printf("【清理缓冲区】清理前: sending_len=%d, ack_cnt=%d, cleaned_len=%d\n", 
                           sock->sending_len, sock->window.wnd_send->ack_cnt, sock->send_cleaned_len);
                    //重新创建一个代替旧的
                    char* new_sending_buf=(char*)malloc(MAX_BUF_SIZE);
                    uint32_t remaining_data_len = sock->sending_len - (sock->window.wnd_send->ack_cnt - sock->send_cleaned_len);
                    printf("【清理缓冲区】剩余数据长度: %d\n", remaining_data_len);
                    if(remaining_data_len > 0){
                        memcpy(new_sending_buf, sock->sending_buf + sock->window.wnd_send->ack_cnt - sock->send_cleaned_len, remaining_data_len);
                    }
                    free(sock->sending_buf);
                    sock->sending_buf=new_sending_buf;
                    sock->sending_len=remaining_data_len;
                    sock->send_cleaned_len=sock->window.wnd_send->ack_cnt;
                    printf("【清理缓冲区】清理后: sending_len=%d, cleaned_len=%d\n", sock->sending_len, sock->send_cleaned_len);
                    pthread_mutex_unlock(&sock->send_lock);
                }
                
                //RTT计算 - 移到清理之后
                sr_packet_t* find_pkt=find_pkt_rtt(send_win,get_ack(pkt));
                if(find_pkt!=NULL){
                    //跳过重传包的RTT计算
                    if(find_pkt->retransmit_count==0&&!rtt->retransmitted_in_flight){
                        //计算Sample_RTT
                        struct timeval nowtime;
                        gettimeofday(&nowtime,NULL);
                        long rtt_sample_us=(nowtime.tv_sec - find_pkt->send_time.tv_sec) * 1000000L + 
                                (nowtime.tv_usec - find_pkt->send_time.tv_usec);
                        //更新RTO
                        update_RTO(&sock->rtt_stats,rtt_sample_us);
                    }
                    rtt->retransmitted_in_flight=0;
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
    //没有缓存乱序pkt 直接发送ACK
    if(sock->unolen==0){
        uint32_t seq=sock->window.wnd_send->nextseq;
        uint32_t ack=sock->window.wnd_recv->expect_seq;
        uint8_t flags=ACK_FLAG_MASK;
        char* ack_pkt=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,flags,TCP_RECVWN_SIZE-(sock->unolen)*MAX_DLEN,0,NULL,0);
        sendToLayer3(ack_pkt,DEFAULT_HEADER_LEN);
    }
    else{
        int len=sock->unolen;
        //先对缓存的乱序pkt按seq进行排序
        for(int i=0;i<len;i++){
            for(int j=i+1;j<len;j++){
                if(get_seq(sock->unorder[i])>get_seq(sock->unorder[j])){
                    my_swap(&sock->unorder[i],&sock->unorder[j]);
                }
            }
        }
        //排序完后 逐个遍历看是否有与expectseq相符的
        int index=0;
        for(;index<len;index++){
            if(get_seq(sock->unorder[index])==sock->window.wnd_recv->expect_seq){
                //那么就将这个pkt放入接收缓冲区中
                char* pkt=sock->unorder[index];
                printf("收到数据 seq=%d\n",get_seq(pkt));
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
                    //直接追加到接收缓冲区末尾 这里recv_len还没有更新 pkt+get_hlen(pkt) 就是直接从数据位置开始
                    memcpy(sock->received_buf + sock->received_len, pkt + get_hlen(pkt), dlen);
                    printf("在接收缓冲区末尾放入大小为dlen=%d的数据\n",dlen);
                    //成功接收数据 buf以及窗口等数据更新
                    sock->window.wnd_recv->avail_wnd_size-=dlen;
                    sock->window.wnd_recv->expect_seq+=dlen;
                    sock->received_len+=dlen;
                    sock->window.wnd_send->nextseq=get_ack(pkt);//更新发送部分
                    pthread_mutex_unlock(&sock->recv_lock);
                }
            }//当前乱序seq小于expectseq 看后面是否有符合的
            else if(get_seq(sock->unorder[index])<(sock->window.wnd_recv->expect_seq)) continue;
            else break;//直到超过仍然没有符合 就跳出
        }
        //部分乱序pkt确认 需要更新乱序缓冲区
        for(int i=index;i<len;i++){
            memcpy(sock->unorder[i-index],sock->unorder[i],sizeof(sock->unorder[i]));//len-index后面剩余的部分先无需更新 因为此时unolen会更新
        }
        sock->unolen-=index;
        //全部更新完成后 再次发送ack确认报文
        uint32_t seq=sock->window.wnd_send->nextseq;
        uint32_t ack=sock->window.wnd_recv->expect_seq;
        uint8_t flags=ACK_FLAG_MASK;
        char* ack_pkt=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,flags,TCP_RECVWN_SIZE-(sock->unolen)*MAX_DLEN,0,NULL,0);
        sendToLayer3(ack_pkt,DEFAULT_HEADER_LEN);
    }
}
void my_swap(char** a,char** b){
    char* tem=*a;
    *a=*b;
    *b=tem;
}
/*重传线程*/
//在受限的SR机制中 只重传
void* resend_pkt_thread(void* arg){
    //线程一直运行 每过固定时间检查每个未确认包 是否超时
    tju_tcp_t* sock=(tju_tcp_t*)arg;
    sender_window_t* send_win=sock->window.wnd_send;
    rtt_stats_t* rtt=&sock->rtt_stats;
    printf("SR重传线程启动 初始重传间隔为=%ldms\n",rtt->timeout_interval_ms);
    while(sock->state==ESTABLISHED||sock->state==CLOSE_WAIT){
        //检查是否需要快速重传 （高优先级）
        if(sock->fast_retransmit){
            handle_fast_retransmit(sock);
            sock->fast_retransmit=0;//重置
        }
        //检查所有未确认包是否超时 （低优先级）
        check_pkt_timeout(sock);
        //间隔时间检查
        //usleep(rtt->timeout_interval_ms*1000/4);// 检查间隔 = 超时间隔/4
        usleep(50000);
    }
    printf("退出重传线程\n");
    return NULL;
}
void check_pkt_timeout(tju_tcp_t* sock){
    sender_window_t* send_win=sock->window.wnd_send;
    rtt_stats_t* rtt=&sock->rtt_stats;
    //检查对应包是否超时
    struct timeval nowtime;
    gettimeofday(&nowtime,NULL);
    int timeout_count=0;//不能无限重传
    for(int i=0;i<MAX_WND_SIZE;i++){
        sr_packet_t* packet=send_win->packets[i];
        if(packet!=NULL&&!packet->acked){
            //计算传输用时
            long elapsed_us=(nowtime.tv_sec - packet->send_time.tv_sec) * 1000000L + 
                     (nowtime.tv_usec - packet->send_time.tv_usec);
            //指数退避判断超时
            long current_timeout=rtt->timeout_interval_ms*(1L<<packet->retransmit_count);
            //限制最大超时时间
            if(current_timeout>60000) current_timeout=60000;
            //判断是否超时
            if(elapsed_us>current_timeout){//超时
                //重传 附上当前时间
                handle_timeout_pkt(sock,packet,nowtime);
            }
        }
    }
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
    //缓存到缓冲区中
    uint32_t slot=seq_num%MAX_WND_SIZE;//使用序列号映射
    send_win->packets[slot]=packet;
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
        printf("首次RTT测量: R1 = %ldμs\n", samplertt_us);
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
        
        printf("后续RTT测量: 样本=%ldμs, 前SRTT=%ldμs, 前DevRTT=%ldμs\n", 
               samplertt_us, prev_srtt, prev_devrtt);
        
        // α = 0.125 代入得 SRTT = (7/8) * SRTT + (1/8) * RTT
        rtt->estmated_rtt = (7 * prev_srtt + samplertt_us) / 8;
        
        // β = 1/4 代入得 DevRTT = (3/4) * DevRTT + (1/4) * |RTT - SRTT|
        long rtt_diff = (samplertt_us > prev_srtt) ? 
                       (samplertt_us - prev_srtt) : (prev_srtt - samplertt_us);
        rtt->dev_rtt = (3 * prev_devrtt + rtt_diff) / 4;
        
        printf("RTT更新完成: SRTT=%ldμs, DevRTT=%ldμs, |RTT-SRTT|=%ldμs\n",
               rtt->estmated_rtt, rtt->dev_rtt, rtt_diff);
    }
    
    /*RTO计算部分*/  
    // μ = 1, ∂ = 4，代入得 RTO = SRTT + 4 * DevRTT              
    long rto_us = rtt->estmated_rtt + 4 * rtt->dev_rtt;
    
    // 对RTO进行检查 有最大最小值
    if (rto_us < 100000) {
        printf("RTO边界检查: %ldμs < 100ms, 调整为100ms\n", rto_us);
        rto_us = 100000;  // 最小RTO = 100ms
    }
    if (rto_us > 60000000) {
        printf("RTO边界检查: %ldμs > 60s, 调整为60s\n", rto_us);
        rto_us = 60000000;  // 最大RTO = 60s
    }
    
    // 更新timeval结构
    rtt->timeout.tv_sec = rto_us / 1000000L;
    rtt->timeout.tv_usec = rto_us % 1000000L;
    
    // 更新毫秒单位（用于显示和检查）
    rtt->estimated_rtt_ms = rtt->estmated_rtt / 1000;
    rtt->dev_rtt_ms = rtt->dev_rtt / 1000;
    rtt->timeout_interval_ms = rto_us / 1000;
    rtt->rto_ms=rtt->timeout_interval_ms;
    printf("RTO更新完成: RTO=%ldms\n", rtt->timeout_interval_ms);
}
/*重传数据包*/
void handle_timeout_pkt(tju_tcp_t* sock,sr_packet_t* packet,struct timeval nowtime){
    sender_window_t* send_win=sock->window.wnd_send;
    rtt_stats_t* rtt=&sock->rtt_stats;
    //检查最大重传次数 MAX=4
    if(packet->retransmit_count>=4){
        printf("重传失败 达到最大重传次数4次");
        sock->state=CLOSED;
        return;
    }
    //超时后加倍RTO 指数退避
    long old_rto = rtt->timeout_interval_ms;
    rtt->timeout_interval_ms = MIN(rtt->timeout_interval_ms * 2, 60000);
    rtt->retransmitted_in_flight = 1;
    //重传pkt
    char* resend_pkt=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,packet->seq_num,0,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN+packet->data_len,NO_FLAG,1,0,packet->data,packet->data_len);
    sendToLayer3(resend_pkt,packet->data_len+DEFAULT_HEADER_LEN);
    free(resend_pkt);
    //更新状态
    packet->retransmit_count++;
    packet->send_time=nowtime;
    printf("重传完成: seq=%u, 第%d次, RTO %ld->%ldms\n", 
           packet->seq_num, packet->retransmit_count, old_rto, rtt->timeout_interval_ms);
}
void handle_fast_retransmit(tju_tcp_t* sock){
    sender_window_t* send_win=sock->window.wnd_send;
    pthread_mutex_lock(&send_win->mutex);
    uint32_t retransmit_seq=sock->fast_retransmit_seq;//得到要重传包的seq
    //找到要重传的包
    sr_packet_t* packet=NULL;
    for(int i=0;i<MAX_WND_SIZE;i++){
        if(send_win->packets[i]!=NULL&&send_win->packets[i]->seq_num==retransmit_seq){
            packet=send_win->packets[i];
            break;
        }
    }
    if(packet==NULL){
        printf("快速重传失败:找不到seq=%u\n",retransmit_seq);
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
        sendToLayer3(fast_retrans_pkt,DEFAULT_HEADER_LEN+packet->data_len);
        free(fast_retrans_pkt);
        //更新包状态
        packet->retransmit_count++;
        gettimeofday(&packet->send_time,NULL);//重置发送时间
        //标记有重传包在运输中 避免RTT计算误差
        sock->rtt_stats.retransmitted_in_flight=1;
    }else{
        printf("快速重传失败 pkt构造失败\n");
    }
    pthread_mutex_unlock(&send_win->mutex);


}