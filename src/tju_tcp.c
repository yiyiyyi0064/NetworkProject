#include "tju_tcp.h"
int send_thread=0;
/*
创建 TCP socket 
初始化对应的结构体
设置初始状态为 CLOSED
*/
tju_tcp_t* tju_socket(){
    tju_tcp_t* sock = (tju_tcp_t*)malloc(sizeof(tju_tcp_t));
    sock->state = CLOSED;
    
    pthread_mutex_init(&(sock->send_lock), NULL);
    sock->sending_buf = NULL;
    sock->sending_len = 0;

    pthread_mutex_init(&(sock->recv_lock), NULL);
    sock->received_buf = NULL;
    sock->received_len = 0;
    
    if(pthread_cond_init(&sock->wait_cond, NULL) != 0){
        perror("ERROR condition variable not set\n");
        exit(-1);
    }

    sock->window.wnd_send = NULL;
    sock->window.wnd_recv = NULL;
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
    //阻塞等待，才全连接队列中返回一个可用socket
    while(!flag){
        for(int i=0;i<MAX_SOCK;i++){
            if(acceptqueue[i]!=NULL&&acceptqueue[i]->state==ESTABLISHED){
                new_conn=acceptqueue[i];
                acceptqueue[i]=NULL;//从列表中移除
                flag=1;
                break;
            }
        }
        if(!flag){
            usleep(10);//休眠避免忙等待
        }
    }
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
    if(send_thread==0){
        send_thread=1;
        pthread_t SendThreadId;
        //创建线程用于传输数据
        int id=pthread_create(&SendThreadId,NULL,send_packet,(void*)sock);
    }
    //将数据复制到缓存区中
    pthread_mutex_lock(&(sock->send_lock));//直接设置锁即可 当资源可用时唤醒
    //复制
    memcpy(sock->sending_buf+send_index,(char*)buffer,len);
    sock->sending_len+=len;//更新缓冲区中总数据量
    sock->index+=len;//向缓存区中写入len字节 下一次写入位置向后移len字节
    pthread_mutex_unlock(&(sock->send_lock));
    return 0;
    
}

void* send_packet(tju_tcp_t* sock){
    while (1){
        if(sock->window.wnd_send->nextseq<sock->send_index){//检查是否有数据待发送       
            while (pthread_mutex_lock(&(sock->send_lock))!=0);
            //计算当前未发送的数据长度
            int unlen=sock->sending_len-(sock->window.wnd_send->nextseq-sock->window.wnd_send->base);
            //取min 未发送与能发送的最大长度
            int send_byte=min(unlen,MAX_DLEN);
            //动态调整中国 最多还能发送多少字节
            int leftlen=sock->window.wnd_send->rwnd-(sock->window.wnd_send->nextseq-sock->window.wnd_send->base);
            send_byte=send_byte>leftlen?leftlen:send_byte//取小
            /*具体实现发送数据包 滑动窗口*/
            if(sock->window.wnd_send->nextseq-sock->window.wnd_send->base+send_byte<=sock->window.wnd_send->rwnd){//检查当前传输是否超出接受窗口
                //获取当前序列号并组装报文
                uint32_t seq=sock->window.wnd_send->nextseq;
                uint32_t ack=seq+send_byte;//这个变量怎么确定
                uint8_t flag=ACK_FLAG_MASK;
                char* data=(char*)malloc(MAX_DLEN);
                //这个地方没太看懂
                memcpy(data,sock->sending_buf+sock->window.wnd_send->nextseq,send_byte);
                //创建TCP报文
                uint16_t* pkt_len=send_byte+DEFAULT_HEADER_LEN;//本次packet的字节长度
                tju_packet_t* senddata = create_packet(sock->established_local_addr.port, sock->established_remote_addr.port,seq, ack, DEFAULT_HEADER_LEN, pkt_len, flag, 0, 0, data, len);
                char* ack_packet=create_packet_buf(sock->established_local_addr.port, sock->established_remote_addr.port,seq, ack, DEFAULT_HEADER_LEN, pkt_len, flag, 0, 0, data, len);
                sendToLayer3(ack_packet,pkt_len);
                sock->window.wnd_send->nextseq+=send_byte;//更新nextseq
                gettimeofday(&buf_resend[sock->packetr]->sent_time,NULL);//更新时间戳
                buf_resend[sock->packetr]=senddata;
                sock->packetr=(sock->packetr+1)%SENDWND_SIZE;//更新重传队列尾部index
                //快速重传重置
                if (len == (int)(sock->window.wnd_send->nextseq - sock->window.wnd_send->base)){
                    gettimeofday(&(sock->window.wnd_send->send_time), NULL);
                    // 重置ACK计数器
                    while(pthread_mutex_lock(&(sock->window.wnd_send->ack_cnt_lock)) != 0);
                    sock->window.wnd_send->ack_cnt = 0;
                    pthread_mutex_unlock(&(sock->window.wnd_send->ack_cnt_lock));
                    }
            }
        }
        
    }
    
}
int tju_recv(tju_tcp_t* sock, void *buffer, int len){
    while(sock->received_len<=0){
        // 阻塞
    }

    while(pthread_mutex_lock(&(sock->recv_lock)) != 0); // 加锁

    int read_len = 0;
    if (sock->received_len >= len){ // 从中读取len长度的数据
        read_len = len;
    }else{
        read_len = sock->received_len; // 读取sock->received_len长度的数据(全读出来)
    }

    memcpy(buffer, sock->received_buf, read_len);

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

    return 0;
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
            //移动到全连接表
            acceptqueue[hashval] = synsock;
            synqueue[hashval] = NULL;
        }
    }
    /*第三次握手 client*/
    else if(sock->state==SYN_SENT&&(flags == (SYN_FLAG_MASK | ACK_FLAG_MASK)) ){
        uint32_t seq=get_ack(pkt);//seq=y
        uint32_t ack=get_seq(pkt)+1;//ack=x+1
        sock->state=ESTABLISHED;
        char* ack_flags=create_packet_buf(get_dst(pkt),get_src(pkt),seq,ack,DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, ACK_FLAG_MASK, 1, 0, NULL, 0);
        sendToLayer3(ack_flags,DEFAULT_HEADER_LEN);
    }
    /*连接关闭 四次挥手*/
     /*第二次挥手*/
    else if(sock->state==ESTABLISHED){
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
        /*建立连接后收到数据包*/
        else if(flags&ACK_FLAG_MASK){
            char hostname[8];
            //获取本机主机名 然后分情况处理
            gethostname(hostname,8);
            if(strcmp(hostname,"server")==0){
                serverrdt(sock,pkt);
            }
            else{
                clientrdt(sock,pkt);
            }
            
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
    // 把收到的数据放到接受缓冲区
    uint32_t data_len = get_plen(pkt) - DEFAULT_HEADER_LEN;
    while(pthread_mutex_lock(&(sock->recv_lock)) != 0); // 加锁

    if(sock->received_buf == NULL){
        sock->received_buf = malloc(data_len);
    }else {
        sock->received_buf = realloc(sock->received_buf, sock->received_len + data_len);
    }
    memcpy(sock->received_buf + sock->received_len, pkt + DEFAULT_HEADER_LEN, data_len);
    sock->received_len += data_len;

    pthread_mutex_unlock(&(sock->recv_lock)); // 解锁
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
        time_point=clock();
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
    established_socks[hashval]=synsock;//加入ehash列表
    synqueue[hashval]=synsock;
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
/*发送端处理数据包*/
void serverrdt(tju_tcp_t* sock,char* pkt){
    /*处理重复的包 序列号小于期望值 重新发送ACK*/
    if(get_seq(pkt)<(sock->window.wnd_recv->expect_seq)){
        //发送ACK
        uint32_t seq=sock->window.wnd_send->nextseq;
        uint32_t ack=sock->window.wnd_recv->expect_seq;
        uint8_t flags=ACK_FLAG_MASK;
        /*流量控制部分，动态调整返回rwnd*/
        char* ack_flags=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,flags,SENDWND_SIZE-(sock->unolen)*MAX_DLEN,0,NULL,0);
    }
    /*处理乱序到达数据包 序列号大于期望值  缓存乱序包 重新发送ACK 并更新接收窗口*/
    else if(get_seq(pkt)>(sock->window.wnd_recv->expect_seq)){
        uint32_t seq=sock->window.wnd_send->nextseq;
        uint32_t ack=sock->window.wnd_recv->expect_seq;
        uint8_t flags=ACK_FLAG_MASK;
        char ack_flags=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,flags,SENDWND_SIZE-(sock->unolen)*MAX_DLEN,0,NULL,0);
        sendToLayer3(ack_flags,DEFAULT_HEADER_LEN);
        //将乱序数据包存入乱序队列 这里实现就直接简单插入 
        //要是可以升级的话可以维护一个b树 
        if(sock->unolen>100) return; //满了就丢弃 
        sock->unolen++;
        memcpy(sock->unorder[sock->unolen],pkt,get_plen(pkt));
    }
    /*处理正常按序到达数据包 序列号等于期望值 */
    else{
        //更新expectseq 加上获取的data长度 就是下一个包的seq
        sock->window.wnd_recv->expect_seq+=get_plen(pkt)-DEFAULT_HEADER_LEN; 
        //更新发送方nextseq  
        sock->window.wnd_send->nextseq=get_ack(pkt);
        uint32_t data_len = get_plen(pkt) - DEFAULT_HEADER_LEN;
        //将数据输入buffer中
        while (pthread_mutex_lock(&(sock->recv_lock)) != 0); // 加锁
        if (sock->received_buf == NULL){
            sock->received_buf = malloc(data_len);
        }
        else{
            sock->received_buf = realloc(sock->received_buf, sock->received_len + data_len);
        }
        memcpy(sock->received_buf + sock->received_len, pkt + DEFAULT_HEADER_LEN, data_len);    
        sock->received_len += data_len;
        pthread_mutex_unlock(&(sock->recv_lock)); // 解锁
        /*处理乱序的数据包 接下来比较复杂一点*/
        //没有缓存乱序数据包 那么直接发送ACK即可
        if(sock->unolen==0){
            uint32_t seq=get_ack(pkt);//回复按照pkt的ack来
            uint32_t ack=sock->window.wnd_recv->expect_seq;
            uint8_t flags=ACK_FLAG_MASK;
            char* ack_flags=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,flags,SENDWND_SIZE-(sock->unolen)*MAX_DLEN,0,NULL,0);
            sendToLayer3(ack_flags,DEFAULT_HEADER_LEN);       
        }
        else{
        //有缓存乱序数据包 那么要对维护的乱序队列进行处理 找出连续按序的pkt
        /*Step01 对缓存中的乱序数据包的seq有序化*/
        int len=sock->unolen;
        for(int i=0;i<len;i++){
            for(int j=i+1;j<len;j++){
                if(get_seq(sock->unorder[i])>get_seq(sock->unorder[j])){
                    seq_swap(&sock->unorder[i],&sock->unorder[j]);
                }    
            }
        }
        /*Step02 在有序队列中找出所有连续的包*/
        int index=0;
        for(;index<len;index++){
            //不断找到缓存中下一个满足更新后expectseq的包
            if(get_seq(sock->unorder[index]==sock)==sock->window.wnd_recv->expect_seq){
                //更新expectseq
                sock->window.wnd_recv->expect_seq+=get_plen(sock->unorder[index])-DEFAULT_HEADER_LEN;
                //更新nextseq 因为等下回复的时候需要对照seq 那就用对面要求的ack
                sock->window.wnd_send->nextseq=get_ack(sock->unorder[index]);
                //将数据输入buffer中
                while (pthread_mutex_lock(&(sock->recv_lock)) != 0); // 加锁
                if (sock->received_buf == NULL){
                    sock->received_buf = malloc(data_len);
                }else{
                    sock->received_buf = realloc(sock->received_buf, sock->received_len + data_len);
                }
                memcpy(sock->received_buf + sock->received_len, pkt + DEFAULT_HEADER_LEN, data_len);    
                sock->received_len += data_len;
                pthread_mutex_unlock(&(sock->recv_lock)); // 解锁
            }
            else if(get_seq(sock->unorder[index])<(sock->window.wnd_recv->expect_seq)){
                continue;
            }
            /*缓存区中没有可以与当前expectseq吻合的pkt*/
            else break;
        }
        /*将已经确认过的pkt退出unorder队列中*/
        for(int i=index;i<len;i++){
            //先将后面还没有确认的pkt移到前面
            memcpy(sock->unorder[i-index],sock->unorder[i],sizeof(sock->unorder[i]));//其实是将MAX-LEN拷贝过去
        }
        sock->unolen-=index;
        /*最后再将更新后的expectseq发送回去*/
        uint32_t seq=sock->window.wnd_send->nextseq;
        uint32_t ack=sock->window.wnd_recv->expect_seq;
        uint8_t flags=ACK_FLAG_MASK;
        char* ack_flags=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,flags,SENDWND_SIZE-(sock->unolen)*MAX_DLEN,0,NULL,0);
        sendToLayer3(ack_flags,DEFAULT_HEADER_LEN); 
        }
    }
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
void seq_swap(char** pkt_a,char** pkt_b){
    uint16_t len_a=get_plen(pkt_a);
    uint16_t len_b=get_plen(pkt_b);
    char* ret=(char*)malloc(MAX_LEN);
    //交换
    memcpy(ret,pkt_a,len_a);
    memcpy(a,b,len_b);
    memcpy(b,ret,len_b);
}