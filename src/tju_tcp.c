#include "tju_tcp.h"

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
    //向客户端发送SYN
    //tju_send(sock,NULL,0,SYN_FLAG_MASK);
    uint32_t seq=0;//初始化序列号 ISN
    uint32_t ack=0;//其实这里应该没有ack的
    //send_tcp_flags(sock,SYN_FLAG_MASK,seq,ack); 
    char* syn_flags=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, SYN_FLAG_MASK, 1, 0, NULL, 0);
    sendToLayer3(syn_flags,DEFAULT_HEADER_LEN);
    sock->state=SYN_SENT;
    int hashval = cal_hash(local_addr.ip, local_addr.port, target_addr.ip, target_addr.port);
        established_socks[hashval] = sock;
    //循环等待直到接收到SYN+ACK
    // while(sock->state!=ESTABLISHED){
    //     usleep(10);
    // }
    //优化 不直接使用usleep函数 设置简单超时重传机制
    //其实就是简单忙等待 ppt提供的有点没看懂
    struct timeval start,current;
    long timeout=2;
    gettimeofday(&start,NULL);
    while(sock->state!=ESTABLISHED){
        gettimeofday(&current,NULL);
        long elapsed = (current.tv_sec - start.tv_sec) +   
                       (current.tv_usec - start.tv_usec) / 1000000.0; //
        if(elapsed>timeout){
            //超时重传
            sendToLayer3(syn_flags,DEFAULT_HEADER_LEN);
            gettimeofday(&start,NULL);//将start重置为当前时间
        }
    }
    // 将建立了连接的socket放入内核 已建立连接哈希表中
    // int hashval = cal_hash(local_addr.ip, local_addr.port, target_addr.ip, target_addr.port);
    // established_socks[hashval] = sock;
    return 0;
}

int tju_send(tju_tcp_t* sock, const void *buffer, int len){
    // 这里当然不能直接简单地调用sendToLayer3
    char* data = malloc(len);
    memcpy(data, buffer, len);

    char* msg;
    uint32_t seq = 464;
    uint16_t plen = DEFAULT_HEADER_LEN + len;

    msg = create_packet_buf(sock->established_local_addr.port, sock->established_remote_addr.port, seq, 0, 
              DEFAULT_HEADER_LEN, plen, NO_FLAG, 1, 0, data, len);

    sendToLayer3(msg, plen);
    
    return 0;
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
    
    uint32_t data_len = get_plen(pkt) - DEFAULT_HEADER_LEN;
    uint8_t flags=get_flags(pkt);//得到header中的标志位
    uint32_t seq_num=get_seq(pkt);
    uint32_t ack_num=get_ack(pkt);
    //进行识别 
    //注意这里还要判断是否是已经连接的 直接使用state识别
    if(sock->state==ESTABLISHED){
    // server来说 接受到FIN 发送ACK 进入CLOSE_WAIT
    if(flags&FIN_FLAG_MASK){
        uint32_t ack=get_seq(pkt)+1;
        uint32_t seq=get_ack(pkt);
        sock->state=CLOSE_WAIT;
        //tju_send(sock,NULL,0,ACK_FLAG_MASK);
        send_tcp_packet(sock,ACK_FLAG_MASK,0);
        char* ack_flags=create_packet_buf(get_dst(pkt),get_src(pkt),seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,ACK_FLAG_MASK,1,0,NULL,0);
        sendToLayer3(ack_flags,DEFAULT_HEADER_LEN);
        //此时socket处于半关闭状态 server还可以发送数据
        //server需要将剩余的数据处理完 再调用close发送FIN
        usleep(500000);//等待数据传输完成
        //等待完成后直接发送FIN+ACK 注意ack一致
        char* fin_ack_flags=create_packet_buf(get_dst(pkt),get_src(pkt),0,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,(ACK_FLAG_MASK|FIN_FLAG_MASK),1,0,NULL,0);
        sendToLayer3(fin_ack_flags,DEFAULT_HEADER_LEN);
        sock->state=LAST_ACK;
        return 0;
    }
    
    }else if(sock->state==LISTEN&&(flags&SYN_FLAG_MASK)){
        handle_syn(sock,pkt);//这里的sock是listen socks 
        //这里sock仍然时LISTEN 因为改变状态的是新创建socket
    }else if(sock->state==SYN_SENT&&(flags == (SYN_FLAG_MASK | ACK_FLAG_MASK)) ){
        //这里处理的是客户端的
        uint32_t ack=get_ack(pkt);//ack=x+1
        uint32_t seq=get_seq(pkt);//seq=y
        sock->state=ESTABLISHED;
        //回复ACK 第三次握手
        //tju_send(sock,NULL,0,ACK_FLAG_MASK); 源和端要调换
        char* ack_flags=create_packet_buf(get_dst(pkt),get_src(pkt),ack,seq+1,DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, ACK_FLAG_MASK, 1, 0, NULL, 0);
        sendToLayer3(syn_flags,DEFAULT_HEADER_LEN);
    }
    //
    else if(sock->state==LISTEN&&(flags&ACK_FLAG_MASK)){
        //sock仍然是LISTEN 但已完成三次握手 服务器最后接收ACK
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
    /*下面添加关闭处理*/
    else if(sock->state==FIN_WAIT_1){
        //client接收到ACK 进入FIN_WAIT_2 
        if(flags&ACK_FLAG_MASK){
        sock->state=FIN_WAIT_2;
        }//client和server同时关闭的情况
        else if (flags&FIN_FLAG_MASK)
        {
            //收到FIN 继续发送ACK
            uint32_t ack=get_seq(pkt)+1;
            uint32_t seq=1;//此时seq变为1
            char* ack_flags=create_packet_buf(get_dst(pkt),get_src(pkt),seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,ACK_FLAG_MASK,1,0,NULL,0);
            sendToLayer3(ack_flags,DEFAULT_HEADER_LEN);
            //状态变为closing
            sock->state=CLOSING;
        }
        
    }else if(sock->state==FIN_WAIT_2&&(flags==(FIN_FLAG_MASK|ACK_FLAG_MASK))){
        /*client接收到FIN+ACK 进入TIME_WAIT 并发送最后一个ACK
        并启动2MSL时间定时器 超时后进进入closed 彻底关闭连接                     */
        sock->state=TIME_WAIT;//只需要等待是否超时即可
        //tju_send(sock,NULL,0,ACK_FLAG_MASK);
        uint32_t seq=get_ack(pkt);
        uint32_t ack=get_seq(pkt)+1;
        char* ack_flags=create_packet_buf(get_dst(pkt),get_src(pkt),seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,ACK_FLAG_MASK,1,0,NULL,0);
        sendToLayer3(ack_flags,DEFAULT_HEADER_LEN);
        //启动2MSL定时器 状态变为CLOSED 当然也可以直接休息sleep
        timer_2msl(sock);
        //usleep(2000000);
        
    }else if(sock->state==LAST_ACK&&(flags&ACK_FLAG_MASK)){
        sock->state=CLOSED;
        //将socket标记为可释放状态
        sock->to_be_free=1;
    }//同时处于closing收到ack
    else if(sock->state==CLOSING){
        if(flags==ACK_FLAG_MASK&&get_ack(pkt)==1)//防止随意关闭
        {
            sock->state=TIME_WAIT;
            timer_2msl(sock);
            //usleep(2000000);
        }
    }
    // 把收到的数据放到接受缓冲区
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
   struct timeval current_time,point_time;
    if(sock->state==ESTABLISHED){
        //发送FIN报文
        //tju_send(sock,NULL,0,FIN_FLAG_MASK);
        //这里的seq从收到的pkt得来
        uint32_t seq=0;
        uint32_t ack=0;
        char* fin_flags=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN,DEFAULT_HEADER_LEN,FIN_FLAG_MASK,1,0,NULL,0);
        //将socket状态改为FIN_WAIT_1
        sendToLayer3(fin_flags,DEFAULT_HEADER_LEN);
        sock->state=FIN_WAIT_1;
    }else if (sock->state==LAST_ACK)
    {
        gettimeofday(&current_time,NULL);
    }
    
    //阻塞等待直到状态变CLOSED
    //ps这里先直接用休眠简单充当一下定时器
    //这里也要支持超时重传 也是简单的忙等待
    // while (sock->state!=CLOSED)
    // {
    //     usleep(1000);
    // }
    while(sock->state!=CLOSED){
        gettimeofday(&point_time,NULL);
        if((point_time-current_time)>=8){
            //重传当前
            printf("重传");
            gettimeofday(&current_time,NULL);
        }
    }
    //关闭 释放资源
    if(sock->to_be_free==1)
    free_socket_resources(sock);
    
    return 0;
}
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
    uint32_t client_seq = get_seq(pkt); // 客户端发来的 seq
    //tju_send(synsock,NULL,0,SYN_FLAG_MASK|ACK_FLAG_MASK);
    //这里是发server
    int seq=get_seq(pkt);//ack=x+1 seq=y 随机设置为0
    //send_tcp_flags(synsock,SYN_FLAG_MASK|ACK_FLAG_MASK,0,seq+1);
    char* syn_ack_flags=create_packet_buf(get_dst(pkt),get_src(pkt),0,seq+1,DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, (SYN_FLAG_MASK|ACK_FLAG_MASK), 1, 0, NULL, 0);
    sendToLayer3(syn_ack_flags,DEFAULT_HEADER_LEN);
    //这里要考虑进制计算 这两个位置同时置1 00010010
}
//半连接表的插入/删除操作
void insert_half_conn(uint32_t local_ip, uint16_t local_port,
                      uint32_t remote_ip, uint16_t remote_port,
                      tju_tcp_t* conn) {
    int hashval = cal_hash(local_ip, local_port, remote_ip, remote_port);
    synqueue[hashval] = conn;
}

tju_tcp_t* find_half_conn(uint32_t local_ip, uint16_t local_port,
                          uint32_t remote_ip, uint16_t remote_port) {
    int hashval = cal_hash(local_ip, local_port, remote_ip, remote_port);
    return synqueue[hashval];
}

void remove_half_conn(uint32_t local_ip, uint16_t local_port,
                      uint32_t remote_ip, uint16_t remote_port) {
    int hashval = cal_hash(local_ip, local_port, remote_ip, remote_port);
    synqueue[hashval] = NULL;
}
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
//添加定时器机制
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

// // 发送TCP报文的通用函数
// void send_tcp_flags(tju_tcp_t* sock, uint8_t flags, uint32_t seq,uint32_t ack) {
    
//     char* packet_flags=create_packet_buf(sock->established_local_addr.port,sock->established_remote_addr.port,seq,ack,DEFAULT_HEADER_LEN, DEFAULT_HEADER_LEN, flags, 1, 0, NULL, 0);
//     sendToLayer3(packet_flags, DEFAULT_HEADER_LEN);
    
// }
//用于生成随机端口 1024-65535
uint16_t random_port(){
    srand((unsigned int) time(NULL));
    uint16_t port=(rand()%(65535-1024+1))+1024;
    return port;
}