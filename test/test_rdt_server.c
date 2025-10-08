#include "tju_tcp.h"
#include <string.h>
#include <signal.h>

void sleep_no_wake(int sec){  
    do{        
        printf("Interrupted\n");
        sec =sleep(sec);
    }while(sec > 0);             
}

int main(int argc, char **argv) {
    // 开启仿真环境 
    startSimulation();
    
    tju_tcp_t* my_server = tju_socket();
    
    tju_sock_addr bind_addr;
    bind_addr.ip = inet_network("172.17.0.3");
    bind_addr.port = 1234;
    
    tju_bind(my_server, bind_addr);

    tju_listen(my_server);

    tju_tcp_t* new_conn = tju_accept(my_server);
    printf("【TEST】accept返回的socket: %p\n", new_conn);
    printf("【TEST】socket状态: %d\n", new_conn->state);
    printf("【TEST】本地地址: %d, 远程地址: %d\n",
           new_conn->established_local_addr.port,
           new_conn->established_remote_addr.port);
    sleep_no_wake(8);

    for (int i=0; i<50; i++){
        char buf[16];
        tju_recv(new_conn, (void*)buf, 16);
        printf("[RDT TEST] server recv %s", buf);
        fflush(stdout);
    }
    sleep_no_wake(100);
    


    return EXIT_SUCCESS;
}
