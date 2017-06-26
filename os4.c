#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <semaphore.h>
#include <sys/time.h>

#define rowcol 2048
float A[rowcol][rowcol];
float B[rowcol][rowcol];
float C[rowcol][rowcol];
sem_t empty, full, work_sem;

struct timeval requeststart, requestend;
double timeSet(struct timeval start, struct timeval end)
{	return (((double)end.tv_sec + (double)end.tv_usec / 1000000) - ((double)start.tv_sec + (double)start.tv_usec / 1000000)); }

typedef struct 
{
	int row_index;
	int priority;
	double inserttime;
	double deletetime;
}info;
typedef struct 
{
	info *heap;
	int heap_size;
}Heap;
Heap buffer;

void initHeap(int buffer_size)
{
	buffer.heap = (info *)malloc(sizeof(info)*buffer_size);
	buffer.heap_size = 0;
}
void freeHeap()
{
	free(buffer.heap); 
}
void send_insertHeap(info information) //send
{ 
	int i;
	
	gettimeofday(&requestend, NULL);
	information.inserttime = timeSet(requeststart, requestend);
	i = ++(buffer.heap_size);
	while((i != 1) && (information.priority < buffer.heap[i/2].priority)) {
		buffer.heap[i] = buffer.heap[i/2];
		i /= 2;
	}
	buffer.heap[i] = information;
}
info recv_deleteHeap() // recv
{
	int parent, child;
	info information, temp;
	
	information = buffer.heap[1];
	gettimeofday(&requestend, NULL);
	information.deletetime = timeSet(requeststart, requestend);
	temp = buffer.heap[(buffer.heap_size)--];
	parent = 1;
	child = 2;
	while (child <= buffer.heap_size) {
		if ((child < buffer.heap_size) && (buffer.heap[child].priority) > buffer.heap[child+1].priority)
			child++;
		if (temp.priority <= buffer.heap[child].priority)
			break;
		buffer.heap[parent] = buffer.heap[child];
		parent = child;
		child *= 2;
	}
	buffer.heap[parent] = temp;
	return information;
}

void *thread(void *data)
{
	int i, j, sol;
	info information;
	double computationtime = 0;
	struct timeval computationstart, computationend;
	
	while(information.row_index >= 0) {
		sem_wait(&full);
		sem_wait(&work_sem);
		information = recv_deleteHeap();
		sem_post(&work_sem);
		sem_post(&empty);

		if (information.row_index >= 0) {
			printf("%d %d %3.6f %3.6f %3.6f\n",information.row_index, information.priority, information.inserttime, information.deletetime - information.inserttime, information.deletetime);
			gettimeofday(&computationstart, NULL);
			for (i = 0; i < rowcol; i++) {
				sol = 0;
				for (j = 0; j < rowcol; j++)
					sol += A[information.row_index][j] * B[j][i];
				C[information.row_index][i] = sol;
			}
			gettimeofday(&computationend, NULL);
			computationtime += timeSet(computationstart, computationend);
		}
	}
	printf("%ld thread computation time : %3.3f\n",(long int)data, computationtime);
	pthread_exit(NULL);
}

int main(int argc, char* argv[])
{
	int res;
	long int i;
	int Arowindex = 0; // 수행해야될 행
	int threadnum = 1; // 총 스레드
	int buffer_size = 1; //buffer크기
	info information;
	double readtime, totaltime;
	struct timeval totalstart, totalend;
	struct timeval iostart, ioend;
	
	if(argc < 4 || argc > 6) { //예외처리
		printf("세그멘테이션 오류!\n");
		return -1;
	}
	else if(argc == 5) //thread수만 지정 했을때
		threadnum = atoi(argv[4]);
	else if(argc == 6) { //thread수만 , buffer크기 지정 했을때
		threadnum = atoi(argv[4]);
		buffer_size = atoi(argv[5]);
	}
	pthread_t workers[threadnum]; //스레드 생성
	initHeap(buffer_size + 1);
	
	gettimeofday(&totalstart, NULL);
	
	res = sem_init(&work_sem, 0, 1); //세마포 초기화
	if(res != 0) {
		perror("semaphore initialization failed");
		exit(EXIT_FAILURE);
	}
	res = sem_init(&empty, 0, buffer_size);
	if(res != 0) {
		perror("semaphore initialization failed");
		exit(EXIT_FAILURE);
	}
	res = sem_init(&full, 0, 0);
	if(res != 0) {
		perror("semaphore initialization failed");
		exit(EXIT_FAILURE);
	}
	
	for (i = 0; i < threadnum; i++) { //스레드 생성
		res = pthread_create(&workers[i], NULL, thread, (void *)i);
		if (res != 0) {
		perror("thread creation failed");
		exit(EXIT_FAILURE);
		}
	}

	//파일 읽기
	gettimeofday(&iostart, NULL);
	int fa, fb;
	fa = open(argv[1], O_RDONLY);
	fb = open(argv[2], O_RDONLY);
	for(i=0; i<rowcol; i++) {
		read(fa, &(A[i][0]), sizeof(float) * rowcol);
		read(fb, &(B[i][0]), sizeof(float) * rowcol);
	}
	close(fa);
	close(fb);
	gettimeofday(&ioend, NULL);
	readtime = timeSet(iostart, ioend);
	
	gettimeofday(&requeststart, NULL); //요청 시작 시간
	
	while (Arowindex < rowcol) {
		information.row_index = Arowindex;
		if (Arowindex%10 == 0)
			information.priority = 0;
		else
			information.priority = 1;
		sem_wait(&empty);
		send_insertHeap(information);
		sem_post(&full);
		Arowindex++;
	}

	//workers종료
	information.row_index = -1;
	information.priority = 2;
	for(i = 0; i < threadnum; i++) {	
		sem_wait(&empty);
		send_insertHeap(information);
		sem_post(&full);
	}
	
	//join
	for (i = 0; i < threadnum; i++) {
		res = pthread_join(workers[i], NULL);
		if (res != 0) {
			perror("thread join failed");
			exit(EXIT_FAILURE);
		}
	}
	
	freeHeap();
	sem_destroy(&work_sem);
	sem_destroy(&empty);
	sem_destroy(&full);

	//파일 쓰기
	int fc;
	fc = open(argv[3], O_RDWR | O_CREAT, 0644);
	for(i=0; i<rowcol; i++)
		write(fc, &(C[i][0]), sizeof(float) * rowcol);
	close(fc);
	gettimeofday(&totalend, NULL);
	totaltime = timeSet(totalstart, totalend);
	
	printf("read time : %3.3f\n", readtime);
	printf("total time : %3.3f\n", totaltime);
	
	return 0;
}
