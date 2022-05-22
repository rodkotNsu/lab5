#include <iostream>
#include <mpi/mpi.h>
#include <cmath>
#include <cstdlib>
#include <vector>

#define LIST_TASKS_COUNT 4
#define DEFAULT_TASKS_COUNT 100

#define REQUEST_TAG 1
#define RESPONSE_TAG 2
#define TASK_TAG 3

#define NO_TASKS 2
#define STOP_THREAD -1
#define L 77777

#define DEFAULT_TASK_VALUE 0

struct Task {
    int repeatNumber;
    explicit Task(int num) : repeatNumber(num) {}
    Task() : Task(DEFAULT_TASK_VALUE) {}
};

using namespace std;

int countTH;
int currentTH;
int currentTask;
int currentIteration;

int inadmissibleValue;

double globalResult;

double ImbalanceTime;
double ImbalancePortion;

int tasksReceived;
int tasksSent;

vector<Task> TaskList;

pthread_t receiverThread;
pthread_t executorThread;

pthread_mutex_t executorMutex;
pthread_mutex_t receiverMutex;

void errorHandler(string message){
    cerr<<message<<endl;
    MPI_Finalize();
    exit(EXIT_FAILURE);
}

void printResults(double& timeSpent){
    cout << "Process rank:\t" << currentTH << "; Iteration:\t" << currentIteration << "; Tasks completed:\t" << currentTask << "; Global result:\t" << globalResult
         << "; Time spent:\t" << timeSpent << "; Task sent:\t" << tasksSent << "; Task received:\t" << tasksReceived << endl;
}

void createTaskList(){
    TaskList.clear();
    for (int i = 0; i < DEFAULT_TASKS_COUNT; ++i) {
        TaskList.emplace_back(abs(DEFAULT_TASKS_COUNT/2 - i) * abs(currentTH - currentIteration % countTH) * L);
    }
}

void subTask1(Task task, double& intermediateValue){
    intermediateValue = pow(sqrt(task.repeatNumber) * sqrt(task.repeatNumber), -3.4);
}

void subTask2(double& intermediateValue){
    intermediateValue = intermediateValue / 29062001.47;
}

void subTaskFinal(double& intermediateValue){
    globalResult = globalResult + 1e9 * pow(intermediateValue, 1/2);
}
//Выполняет задание
void executeTasks(Task task){
    double intermediateValue;

    for(int i = 0; i  < task.repeatNumber; ++i){
        subTask1(task, intermediateValue);
        subTask2(intermediateValue);
        subTaskFinal(intermediateValue);
    }
}

void calculateResult(double &timeSpent){
    double maxTime, minTime;
    MPI_Allreduce(&timeSpent, &maxTime, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
    MPI_Allreduce(&timeSpent, &minTime, 1, MPI_DOUBLE, MPI_MIN, MPI_COMM_WORLD);

    ImbalanceTime = maxTime - minTime;
    ImbalancePortion = ImbalanceTime / maxTime * 100;
}

int receiveTasks(int requestTH){
    if(requestTH == currentTH){
        return NO_TASKS;
    }
    int code = 1;

    MPI_Send(&code,1, MPI_INT, requestTH, REQUEST_TAG, MPI_COMM_WORLD);

    MPI_Recv(&code,1, MPI_INT, requestTH, RESPONSE_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    if(code == 0){
        return NO_TASKS;
    }

    Task receivedTask;
    MPI_Recv(&receivedTask, sizeof(receivedTask), MPI_BYTE, requestTH, TASK_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    pthread_mutex_lock(&executorMutex);
    TaskList.push_back(receivedTask);
    pthread_mutex_unlock(&executorMutex);

    tasksReceived++;

    return code;
}

void * doTasks(void * args){
    int requestTH;
    int addition = 0;
    double startTime, endTime, time;

    for(currentIteration = 0; currentIteration < LIST_TASKS_COUNT; ++currentIteration, currentTask = 0, addition = 0){
        tasksReceived = 0;

        pthread_mutex_lock(&executorMutex);
        createTaskList();

        startTime = MPI_Wtime();
        while(addition < countTH){
            while(currentTask < TaskList.size()){
                pthread_mutex_unlock(&executorMutex);
                executeTasks(TaskList[currentTask]);
                pthread_mutex_lock(&executorMutex);

                ++currentTask;
            }

            pthread_mutex_unlock(&executorMutex);
            requestTH = (currentTH + addition) % countTH;
            if(receiveTasks(requestTH) == NO_TASKS){
                ++addition;
            }
            pthread_mutex_lock(&executorMutex);
        }

        pthread_mutex_unlock(&executorMutex);
        endTime = MPI_Wtime();

        time = endTime - startTime;

        printResults(time);

        tasksSent = 0;
        MPI_Barrier(MPI_COMM_WORLD);

        calculateResult(time);



        if(currentTH == countTH-1){
            std::cout << "Imbalance time: " << ImbalanceTime << "; Imbalance portion: " << ImbalancePortion << std::endl;
        }
    }

    int stopFlag = STOP_THREAD;
    MPI_Send(&stopFlag, 1, MPI_INT, currentTH, REQUEST_TAG, MPI_COMM_WORLD);

    pthread_exit(nullptr);
}

void* consumerRequests(void* args){
    int code;
    MPI_Status status;
    while(currentIteration < LIST_TASKS_COUNT){
        MPI_Recv(&code, 1, MPI_INT, MPI_ANY_SOURCE, REQUEST_TAG, MPI_COMM_WORLD, &status);
        if(code == STOP_THREAD){
            pthread_exit(nullptr);
        }

        pthread_mutex_lock(&receiverMutex);
     
        if(currentTask + inadmissibleValue > TaskList.size()){
            code = 0;
        }
        pthread_mutex_unlock(&receiverMutex);

        MPI_Send(&code, 1, MPI_INT, status.MPI_SOURCE, RESPONSE_TAG, MPI_COMM_WORLD);
        if(code == 0){
            continue;
        }

        pthread_mutex_lock(&receiverMutex);
        Task taskToSend = TaskList.back();
        TaskList.pop_back();
        pthread_mutex_unlock(&receiverMutex);

        MPI_Send(&taskToSend, sizeof(Task), MPI_BYTE, status.MPI_SOURCE, TASK_TAG, MPI_COMM_WORLD);

        ++tasksSent;
    }

    pthread_exit(nullptr);
}

void start(){
    pthread_mutex_init(&executorMutex, nullptr);
    pthread_mutex_init(&receiverMutex, nullptr);

    int statusReceiver = pthread_create(&receiverThread, nullptr, consumerRequests, nullptr);
    int statusExecutor = pthread_create(&executorThread, nullptr, doTasks, nullptr);

    if(statusReceiver && statusExecutor){
        errorHandler("Ошибка создания потоков");
    }

    statusReceiver = pthread_join(executorThread,nullptr);
    statusExecutor = pthread_join(receiverThread,nullptr);

    if(statusReceiver && statusExecutor){
        errorHandler("Ошибка в присоединении  потоков" );
    }

    pthread_mutex_destroy(&executorMutex);
    pthread_mutex_destroy(&receiverMutex);
}

int main(int argc, char* argv[]){
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    MPI_Comm_size(MPI_COMM_WORLD, &countTH);
    MPI_Comm_rank(MPI_COMM_WORLD, &currentTH);

    inadmissibleValue = countTH + 1;

    start();

    MPI_Finalize();

    return EXIT_SUCCESS;
}