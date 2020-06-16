#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>

#define MAX_K_VALUE 10
#define MAX_N_VALUE 10000
#define MAX_B_VALUE 10000

int results[MAX_N_VALUE][2] = {0};
pthread_t tid_array[MAX_K_VALUE];

int BUFFERS[MAX_K_VALUE][MAX_B_VALUE][3];

int* vectorValue;

struct thread
{
	FILE *mfp;
	FILE *vfp;
	FILE *ifp;
	FILE* rfp;
	
	sem_t sem_mutex;
	sem_t sem_full;
	sem_t sem_empty;
	
	int K;
	int B;
	int n;
	int id;
	
	int in;
	int out;
	
	int flagCount;
	
	int buffer[][3];
};

//Number of lines in a file
int getNumberOfLines(FILE* inputfile)
{
	int lineNumber = 0;
	char chr = fgetc(inputfile);
	while(!feof(inputfile))
	{
		// reads \n
		if(chr == '\n')
		{
			lineNumber++;
		}
		chr = getc(inputfile);
	}
	rewind(inputfile);
	return lineNumber;
	//End reading number of lines
}

//Creating split files
FILE *createSplit(FILE *inputfileptr, int s)
{
	char line[20];
	
	FILE *intfile;
	intfile = tmpfile();

	if(intfile == NULL)
	{
		printf("Tmp file could not be created");
	}
	
	printf("Split file creating: \n");
	for(int i = 0; i < s; i++)
	{
		fgets(line, sizeof(line), inputfileptr);
		fputs(line, intfile);
		printf("%s",line);
	}
	rewind(intfile);
	return intfile;
}

void *thread_start(void *arg)
{
	printf("Mapper has started!\n");
	//Struct creation
	struct thread *p;
	p = (struct thread *) arg;
	
	char line[20];
	char row[5];
	char column[5];
	char value[5];
	int intRow;
	int intCol;
	int intVal;
	
	p->n = getNumberOfLines(p->mfp);
	
	while(fgets(line, sizeof(line), p->mfp))				//!FEOF
	{
		sem_wait(&p->sem_empty);
		sem_wait(&p->sem_mutex);
		
		sscanf(line, "%s %s %s", row, column, value);
		intRow = (int)strtol(row,     (char**)NULL,10);
		intCol = (int)strtol(column,  (char**)NULL,10);
		intVal = (int)strtol(value,   (char**)NULL,10);

		p->buffer[p->in][0] = intRow;
		p->buffer[p->in][1] = intCol;
		p->buffer[p->in][2] = intVal;
		
		p->in++;
		//p->in = (p->in + 1) % p->B;
		
		sem_post(&p->sem_mutex);
		sem_post(&p->sem_full);
	}
	
	p->flagCount++;
	rewind(p->mfp);
	rewind(p->vfp);
	rewind(p->ifp);
	printf("End of mapper\n");
	pthread_exit(NULL);
}

void *thread_reducer(void *arg)
{
	printf("Reducer has started\n");
	
	struct thread *mapper;
	mapper = (struct thread *) arg;
	
	int row;
	int col;
	int val;

	while(mapper->flagCount < mapper->K)
	{
		sem_wait(&mapper->sem_full);
		sem_wait(&mapper->sem_mutex);
	
		row = mapper->buffer[mapper->out][0];
		col = mapper->buffer[mapper->out][1];
		val = mapper->buffer[mapper->out][2];
		
		results[row][1] = results[row][1] + (val * vectorValue[col]);
		
		mapper->out++;
		
		sem_post(&mapper->sem_mutex);
		sem_post(&mapper->sem_empty);
	}
	
	char charValue[20];
	mapper->n = getNumberOfLines(mapper->vfp);

	for(int i = 1; i <= mapper->n; i++)
	{
		results[i][0] = i;
	}
	
	for(int i = 1; i <= mapper->n; i++)
	{
		printf("[%d] [%d]\n", results[i][0], results[i][1]);
		sprintf(charValue, "%d %d\n", results[i][0], results[i][1]);
		fputs(charValue, mapper->rfp);
	}
	
	printf("End of reducer\n");
	pthread_exit(NULL);
}

int main(int argc, char *argv[])
{
	if(argc < 3)
	{
		printf("One of the files or both files are missing!\n");
		exit(-1);
	}

	//Matrix and vector files and Arguments
	FILE *mfp, *vfp, *rfp;
	mfp = fopen(argv[1],"r");
	vfp = fopen(argv[2],"r");
	
	//Result file
	if(argv[3])
	{
		rfp = fopen(argv[3],"w");
	}
	else
	{
		rfp = fopen("resultfile.txt","w");
	}
	
	//K value
	int K = 1;						//If not specified K = 1
	if(argv[4] != NULL)
	{
		K = (int)strtol(argv[4],(char**)NULL,10);
	}
	
	if(K > 10 || K < 0)
	{
		printf("K value is greater or less than expected!\n");
		exit(-1);
	}
	
	int B = 100;
	if(argv[5] != NULL)
	{
		B = (int)strtol(argv[5],(char**)NULL,10);
	}
	
	if(B < 100 || B > 10000)
	{
		printf("B value is greater or less than expected!\n");
		exit(-1);
	}
	
	if(getNumberOfLines(vfp) <= 0)
	{
		printf("Unexpected size for the vector file!\n");
		exit(-1);
	}
	
	if(getNumberOfLines(mfp) <= 0)
	{
		printf("Unexpected size for the matrix file!\n");
		exit(-1);
	}
	//End argument arrangement
	
	//Start
	
	int s = 0, numberOfLines = 0, lastpiece = 0;
	numberOfLines = getNumberOfLines(mfp);
	s = numberOfLines/K;
	
	if(numberOfLines%K != 0)
	{
		lastpiece = numberOfLines%K;
		K++;													//Increase K if the size of last split will be less than s
	}
	
	
	/*This part creates an array to keep tmpfiles.
	 *Checks whether number of ceilings (s) is 0 or not. If it is 0 gives error message and returns -1.
	 *Creates the splits.
	 *Stores the resulting files from each split into an array of file pointers
	*/
	
	FILE *tmparray[K];
	if(s > 0)
	{
		int i = 0;
		for(;i < K; i++)
		{
			if( (i == (K-1) ) && (lastpiece > 0) )
			{
				tmparray[i] = createSplit(mfp,lastpiece);
				break;
			}
			tmparray[i] = createSplit(mfp,s);
		}
	}
	else
	{
		printf("There is something wrong with matrix size or split (K) value!");
		exit(-1);
	}
	
	//Reading vector file and putting values into an array
	char line[20];
	char row[5];
	char value[5];
	int intVal = 0;
	
	int n;
	n = getNumberOfLines(vfp);
	vectorValue = (int*)malloc(n * sizeof(int));
	
	if(vectorValue == NULL)
	{
		printf("Memory not allocated\n");
		exit(-1);
	}
	
	int i = 1;
	while(fgets(line, sizeof(line), vfp))
	{
		sscanf(line, "%s %s", row, value);
		if(i <= n)						//To protect memory from overwriting by malloc
		{
			intVal = (int)strtol(value,(char**)NULL, 10);
			vectorValue[i] = intVal;
			printf("vectorValue[%d] = %d\n", i, vectorValue[i]);
			i++;
		}
	}

	//Creating mapper threads
	int id = 0;
	
	pthread_t tid;
	int tcheck;
	struct thread par;
	
	par.mfp = mfp;
	par.vfp = vfp;
	par.rfp = rfp;
	
	par.n = n;
	par.K = K;
	par.B = B;
	
	par.in  = 0;
	par.out = 0;
	
	par.flagCount = 0;
	
	sem_init(&par.sem_mutex, 0, 1);
	sem_init(&par.sem_full, 0, 0);
	sem_init(&par.sem_empty, 0, B);
	
	for(; id < K; id++)
	{
		if(tmparray[id] != NULL)
		{
			par.ifp = tmparray[id];
		}
		else
		{
			printf("There is something wrong with split files!\n");
			exit(-1);
		}

		par.id = id;
		//tid = tid_array[id];
		tcheck = pthread_create(&(tid), NULL, thread_start, (void *)&par);
		tid_array[id] = tid;
		printf("tpid is %d\n", (int)tcheck);
		
		if(tcheck != 0)
		{
			printf("Thread create failed \n");
			exit(1);
		}
	}
	
	//Reducer thread
	pthread_t rtid;
	printf("par.n is: %d\n", par.n);
	tcheck = pthread_create(&(rtid), NULL, thread_reducer, (void *)&par);
	
	if(tcheck != 0)
	{
		printf("Thread create failed \n");
		exit(1);
	}
	
	tcheck = pthread_join(rtid, NULL);
	
	for(int i = 0; i < n; i++)
	{
		printf("%d\n", results[i][1]);
	}
	
	printf("End of the program!\n");
	//close program
	free(vectorValue);
	fclose(mfp);
	fclose(vfp);
	fclose(rfp);
	exit(0);
}