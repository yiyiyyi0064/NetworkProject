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
    tju_tcp_t* new_conn = (tju_tcp_t*)malloc(sizeof(tju_tcp_t));
    memcpy(new_conn, listen_sock, sizeof(tju_tcp_t));

    tju_sock_addr local_addr, remote_addr;
    /*
     这里涉及到TCP连接的建立
     正常来说应该是收到客户端发来的SYN报文
     从中拿到对端的IP和PORT
     换句话说 下面的处理流程其实不应该放在这里 应该在tju_handle_packet中
    */ 
    remote_addr.ip = inet_network("172.17.0.2");  //具体的IP地址
    remote_addr.port = 5678;  //端口

    local_addr.ip = listen_sock->bind_addr.ip;  //具体的IP地址
    local_addr.port = listen_sock->bind_addr.port;  //端口

    new_conn->established_local_addr = local_addr;
    new_conn->established_remote_addr = remote_addr;

    // 这里应该是经过三次握手后才能修改状态为ESTABLISHED

    new_conn->state = ESTABLISHED;

    // 将新的conn放到内核建立连接的socket哈希表中
    int hashval = cal_hash(local_addr.ip, local_addr.port, remote_addr.ip, remote_addr.port);
    established_socks[hashval] = new_conn;

    // 如果new_conn的创建过程放到了tju_handle_packet中 那么accept怎么拿到这个new_conn呢
    // 在linux中 每个listen socket都维护一个已经完成连接的socket队列
    // 每次调用accept 实际上就是取出这个队列中的一个元素
    // 队列为空,则阻塞 
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
    local_addr.ip = inet_network("172.17.0.2");
    local_addr.port = 5678; // 连接方进行connect连接的时候 内核中是随机分配一个可用的端口
    sock->established_local_addr = local_addr;
    //向客户端发送SYN
    tju_send(sock,NULL,0,SYN_FLAG_MASK);
    sock->state=SYN_SENT;
    //循环等待直到接收到SYN+ACK
    while(sock->state!=ESTABLISHED){
        
    }
    int hashval = cal_hash(local_addr.ip, local_addr.port, target_addr.ip, target_addr.port);
        established_socks[hashval] = sock;
    // 这里也不能直接建立连接 需要经过三次握手
    // 实际在linux中 connect调用后 会进入一个while循环
    // 循环跳出的条件是socket的状态变为ESTABLISHED 表面看上去就是 正在连接中 阻塞
    // 而状态的改变在别的地方进行 在我们这就是tju_handle_packet
    //sock->state = ESTABLISHED;

    // 将建立了连接的socket放入内核 已建立连接哈希表中
    // int hashval = cal_hash(local_addr.ip, local_addr.port, target_addr.ip, target_addr.port);
    // established_socks[hashval] = sock;
    return 0;
}

int tju_send(tju_tcp_t* sock, const void *buffer, int len,uint16_t flags){
    // 这里当然不能直接简单地调用sendToLayer3
    char* data = malloc(len);
    memcpy(data, buffer, len);

    char* msg;
    uint32_t seq = 464;
    uint16_t plen = DEFAULT_HEADER_LEN + len;

    msg = create_packet_buf(sock->established_local_addr.port, sock->established_remote_addr.port, seq, 0, 
              DEFAULT_HEADER_LEN, plen,flags , 1, 0, data, len);

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
    //进行识别 
    //注意这里还要判断是否是已经连接的 直接使用state识别
    if(sock->state=ESTABLISHED){
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
    }else if(sock->state==LISTEN&&flags==SYN_FLAG_MASK){
        handle_syn(sock,pkt);//这里的sock是listen socks
    }else if(sock->state==SYN_SENT&&flags==SYN_FLAG_MASK|ACK_FLAG_MASK){
        //这里处理的是客户端的
        sock->state=ESTABLISHED;
        //回复ACK 第三次握手
        tju_send(sock,NULL,0,ACK_FLAG_MASK);
    }else if(sock->state=SYN_RECV&&flags==ACK_FLAG_MASK){
        //将半连接队列中的socket转入全连接队列
        uint16_t remote_port = get_src(pkt);  // 客户端端口
        uint16_t local_port = get_dst(pkt);   // 服务端端口
        uint32_t remote_ip = inet_network("172.17.0.2");//客户端
        uint32_t local_ip = sock->bind_addr.ip;//sock通道是一直不变的 只是pkt会变
        int hashval = cal_hash(local_ip, local_port, remote_ip, remote_port);
        tju_tcp_t* synsock = half_conn_socks[hashval];
        if(synsock==NULL){
            printf("未找到对应半连接!");
            return;
        }
        //将该socket状态改为ESTABLISHED
        synsock->state=ESTABLISHED;
        //移动到全连接表
        established_socks[hashval] = synsock;
        half_conn_socks[hashval] = NULL;
    }
    
    return 0;
}

int tju_close (tju_tcp_t* sock){
    return 0;
}
void handle_syn(tju_tcp_t* listen_socks,char* pkt){
    //新建socket 直接使用已构造函数
    //tju_tcp_t* new_sock=malloc(sizeof(tju_tcp_t));
    tju_tcp_t* synsock=tju_socket();
    //初始化，并设置state为SYN_RECV
    uint32_t remote_ip = inet_network("172.17.0.2");//由于写定了 故直接写
    synsock->established_local_addr=listen_socks->bind_addr;
    synsock->established_remote_addr.port=get_src(pkt); //确定端口
    synsock->established_remote_addr.ip=remote_ip;
    synsock->state=SYN_RECV;
    //将新建socket放入半连接队列
    int hashval = cal_hash(
        synsock->established_local_addr.ip,
        synsock->established_local_addr.port,
        synsock->established_remote_addr.ip,
        synsock->established_remote_addr.port
    );
    half_conn_socks[hashval]=synsock;
    uint32_t client_seq = get_seq(pkt); // 客户端发来的 seq
    synsock->window.my_seq = rand();    // 服务端随机起始序列号
    synsock->window.my_ack = client_seq + 1; // 确认号 = 客户端 seq+1
    //发送SYN+ACK发送给client
    tju_send(synsock,NULL,0,SYN_FLAG_MASK|ACK_FLAG_MASK);
    //这里要考虑进制计算 这两个位置同时置1 00010010
}
//半连接表的插入/删除操作
void insert_half_conn(uint32_t local_ip, uint16_t local_port,
                      uint32_t remote_ip, uint16_t remote_port,
                      tju_tcp_t* conn) {
    int hashval = cal_hash(local_ip, local_port, remote_ip, remote_port);
    half_socks[hashval] = conn;
}

tju_tcp_t* find_half_conn(uint32_t local_ip, uint16_t local_port,
                          uint32_t remote_ip, uint16_t remote_port) {
    int hashval = cal_hash(local_ip, local_port, remote_ip, remote_port);
    return half_socks[hashval];
}

void remove_half_conn(uint32_t local_ip, uint16_t local_port,
                      uint32_t remote_ip, uint16_t remote_port) {
    int hashval = cal_hash(local_ip, local_port, remote_ip, remote_port);
    half_socks[hashval] = NULL;
}
