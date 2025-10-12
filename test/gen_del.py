import matplotlib.pyplot as plt  
import numpy as np 

def get_dlev():    
    # 文件名  
    input_file_name = 'server.event.trace'   # 读取的源文件  
    output_file_name = 'DELV.trace'  # 输出的目标文件  

    # 提取包含"DELV"的行  
    delv_lines = []  

    # 读取文件并处理每一行  
    with open(input_file_name, 'r') as file:  
        for line in file:  
            if "[DELV]" in line:  
                delv_lines.append(line.strip())  # 去掉行尾的换行符  

    # 将结果写入新文件  
    with open(output_file_name, 'w') as output_file:  
        for delv_line in delv_lines:  
            output_file.write(delv_line + '\n')  # 写入每一行并添加换行符  

    print(f"提取的DELV行已写入到{output_file_name}")


def read_trace(file):
    DELV_dic = {'utctime':[], 'seq':[], 'size':[], 'throughput':[]}

    start_time = 0
    with open(file, 'r', encoding='utf-8') as f:
        for num, line in enumerate(f):
            if(line=='\n'): continue # 跳过空行
            if('DELV' not in line): continue # 跳过非事件行
            line = line.strip('\n')
            line = line.replace('[', '')
            line = line.replace(']', '')
            line_list = line.split(' ')
            info_list = line_list[2:]
            info_list = [item.split(':')[1] for item in info_list]
            if line_list[1] == 'DELV':
                DELV_dic['utctime'].append(int(line_list[0]))
                DELV_dic['seq'].append(int(info_list[0]))
                DELV_dic['size'].append(int(info_list[1])) 

            if start_time==0:
                start_time = int(line_list[0])

    DELV_dic['time'] = [item - start_time for item in DELV_dic['utctime']]
    DELV_dic['time'] = np.divide(DELV_dic['time'], 1000000)

    return DELV_dic

# get_dlev() 

def caculate(FILE_TO_READ):
    DELV_dic = read_trace(FILE_TO_READ)
    FILE_Through = '/vagrant/tju_tcp/test/through_with_wnd.trace'
    # 每间隔1s绘制一次吞吐率
    if len(DELV_dic['utctime']): 
        thrp_intv = 1 # throughput interval
        time_start = DELV_dic['time'][0]
        intvs = int(DELV_dic['time'][-1])
        thrp_list = []
        for i in range(intvs):
            time_end = time_start+thrp_intv
            mark = (DELV_dic['time']>=time_start) & (DELV_dic['time']<time_end)
            payloads_size = np.array(DELV_dic['size'])[mark]
            DELV_dic['throughput'].append(np.sum(payloads_size)*8/thrp_intv) # 单位: bps
            time_start += thrp_intv
        average_throughput = np.mean(DELV_dic['throughput']) if DELV_dic['throughput'] else 0  
        # 打印平均吞吐率  
        print(f"Average Throughput: {average_throughput} bps") 
        with open(FILE_Through, 'a') as file:  
            file.write(f"56MSS: {average_throughput}\n")  

caculate('/vagrant/tju_tcp/test/server.event.trace')
# 0% 11154982.857142856 bps
# 1% 10063856.41025641 bps
# 2% 9021786.363636363 bps
# 3% 7960300.0 bps
# 4% 6658508.474576271 bps
# 5% 6130836.923076923 bps
# 6% 5405343.243243244 bps



# 数据：丢包率和对应的吞吐量  
def throug_with_loss():
    packet_loss_rate = [0, 1, 2, 3, 4, 5, 6]  # % 丢包率  
    throughout = [  
        11154982.857142856,  
        10063856.41025641,  
        9021786.363636363,  
        7960300.0,  
        6658508.474576271,  
        6130836.923076923,  
        5405343.243243244  
    ]  # 吞吐量，bps  

    # 将吞吐量从 bps 转换为 Mbps  
    throughout_mbps = [x / 1_000_000 for x in throughout]  # 转换为 Mbps  

    # 创建折线图  
    plt.figure(figsize=(10, 6))  
    plt.plot(packet_loss_rate, throughout_mbps, marker='o')  

    # 图表标题和标签  
    plt.title('Throughout Variates with Loss')  
    plt.xlabel('Loss (%)')  
    plt.ylabel('Throughout (Mbps)')  
    plt.grid(True)  

    # 显示图表  
    plt.xticks(packet_loss_rate)  # 自定义 x 轴刻度  
    plt.savefig('/vagrant/tju_tcp/test/loss_throughout.png')  # 保存为图片  
    plt.show()  # 显示图表
    

def throug_with_wnd():
    # 定义窗口大小和对应的吞吐率（bps）  
    window_sizes = [8, 16, 24, 36, 48, 56, 64]  
    throughput_values_bps = [  
        2956363.6363636362,  
        5919791.044776119,  
        8161175.510204081,  
        13299696.551724138,
        13495193.548387097,
        13690183.525382041,
        13694153.043456248
    ]  

    # 将吞吐率转换为 Mbps  
    throughput_values_mbps = [value / 1_000_000 for value in throughput_values_bps]  

    # 创建折线图  
    plt.figure(figsize=(10, 6))  
    plt.plot(window_sizes, throughput_values_mbps, marker='o')  

    # 添加标题和标签  
    plt.title('Throughout varies with wnd (Mbps)')  
    plt.xlabel('wnd-szie (MSS)')  
    plt.ylabel('throughout (Mbps)')  

    # 添加网格  
    plt.grid()  

    # 显示图例  
    plt.legend(['throughout'])  
    plt.savefig('/vagrant/tju_tcp/test/wnd_throughout.png')
    # 显示图形  
    plt.show()

throug_with_wnd()