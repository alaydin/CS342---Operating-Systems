#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pthread.h>
#include <time.h>

#define MAX_K_VALUE 10
#define MAX_N_VALUE 10000
int results[MAX_K_VALUE][MAX_N_VALUE][2] = {0};
pthread_t tid_array[MAX_K_VALUE];

struct mapper_thread
{
	FILE *mfp;
	FILE *vfp;
	FILE *ifp;
	
	int K;
	int n;
	int i;
	int resultarray[][2];
};

struct reducer_thread
{
	FILE* rfp;
	
	int K;
	int n;
	int i;
	int resultarray[][2];
};

int getNumberOfLines(FILE* inputfile)
{
	//Number of lines in file
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
	//printf("number of lines in file = %d\n", lineNumber);
	rewind(inputfile);
	return lineNumber;
	//End reading number of lines
}

void *thread_start(void *arg)
{
	printf("Mapper thread started\n");
	//Struct creation
	struct mapper_thread *p;
	p = (struct mapper_thread *) arg;

	//Matrix multipication
	char line[20];
	char row[5];
	char column[5];
	char value[5];
	int intRow = 0;
	int intCol = 0;
	int intVal = 0;
	int* vectorValue;

	p->n = getNumberOfLines(p->mfp);
	printf("n is: %d\n", p->n);
	p->n = getNumberOfLines(p->vfp);
	vectorValue = (int*)malloc(p->n * sizeof(int));
	
	for(int i = 0; i < p->n; i++)
	{
		p->resultarray[i][0] = 0;
		p->resultarray[i][1] = 0;
	}
	
	if(vectorValue == NULL)
	{
		printf("Memory not allocated\n");
		exit(-1);
	}

	int i = 0;
	while(fgets(line, sizeof(line), p->vfp))
	{
		sscanf(line, "%s %s", row, value);
		if(i < p->n)						//To protect memory from overwriting by malloc
		{
			intVal = (int)strtol(value,(char**)NULL, 10);
			vectorValue[i] = intVal;
			printf("vectorValue[%d] = %d\n", i, vectorValue[i]);
			i++;
		}
	}
	i = 0;
	intRow = 0;
	intCol = 0;
	intVal = 0;
	p->n = getNumberOfLines(p->mfp);
	printf("n is %d i is %d\n", p->n, i);
	while(fgets(line, sizeof(line), p->mfp))
	{
		sscanf(line, "%s %s %s", row, column, value);
		if(i < p->n)
		{
			intRow = (int)strtol(row,     (char**)NULL,10) - 1;
			intCol = (int)strtol(column,  (char**)NULL,10) - 1;
			intVal = (int)strtol(value,   (char**)NULL,10);
			if(intVal != 0)
			{
				p->resultarray[intRow][0] = intRow + 1;
				p->resultarray[intRow][1] = p->resultarray[intRow][1] + (intVal * vectorValue[intCol] );
				
				results[p->i][intRow][0] = p->resultarray[intRow][0];
				results[p->i][intRow][1] = p->resultarray[intRow][1];
				printf("row: %d	value: %d\n", results[p->i][intRow][0], results[p->i][intRow][1]);
			}
		}
		i++;
	}
	rewind(p->mfp);
	rewind(p->vfp);
	free(vectorValue);
	printf("Hello there\n");
	pthread_exit(NULL);
}


void *thread_reducer(void *arg)
{
	printf("Reducer has started\n");
	
	struct reducer_thread *p;
	p = (struct reducer_thread *) arg;
	
	for(int i = 0; i < p->K; i++)
	{
		pthread_join(tid_array[i], NULL);
	}
	
	for(int i = 0; i < p->n; i++)
	{
		p->resultarray[i][0] = 0;
		p->resultarray[i][1] = 0;
	}
	
	char charValue[20];
	
	for(int i = 0; i < p->K; i++)
	{
		for(int j = 0; j < p->n; j++)
		{
			if(results[i][j][0] != 0)
			{
				printf("before resultarray row: %d	value: %d\n", p->resultarray[j][0], p->resultarray[j][1]);
				p->resultarray[j][1] = p->resultarray[j][1] + results[i][j][1];
				printf("after resultarray row: %d	value: %d\n", p->resultarray[j][0], p->resultarray[j][1]);
			}
		}
	}
	for(int i = 0; i < p->n; i++)
	{
		p->resultarray[i][0] = i + 1;
	}
	
	for(int i = 0; i < p->n; i++)
	{
		sprintf(charValue, "%d %d\n", p->resultarray[i][0], p->resultarray[i][1]);
		fputs(charValue, p->rfp);
	}
	
	pthread_exit(NULL);
}


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

int main(int argc, char *argv[])
{
	if(argc < 3)
	{
		printf("One of the files or both files are missing!\n");
		exit(-1);
	}
	printf ("mv started\n");
	
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
	printf("K is: %d\n", K);
	if(K > 10 || K < 0)
	{
		printf("K value is greater than expected\n");
		exit(-1);
	}
	
	if(getNumberOfLines(vfp) <= 0)
	{
		printf("Unexpected size for the vector file!\n");
		exit(-1);
	}
	//End argument arrangement
	
	//Clock start
	clock_t start, end;
	double cpu_time_used;
	start = clock();
	
	int s = 0, numberOfLines = 0, lastpiece = 0;
	numberOfLines = getNumberOfLines(mfp);
	s = numberOfLines/K;
	if(numberOfLines%K != 0)
	{
		lastpiece = numberOfLines%K;
		K++;							//Increase K if the size of last split will be less than s
	}

	//char *numbers[10] = {"intfile0.txt","intfile1.txt","intfile2.txt","intfile3.txt","intfile4.txt"
								//,"intfile5.txt","intfile6.txt","intfile7.txt","intfile8.txt","intfile9.txt"};
	
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
	//End of the part
	
	//Create Threads
	pthread_t tid;
	int tcheck;
	struct mapper_thread par;
	int i = 0;
	int n = getNumberOfLines(vfp);
	
	par.mfp = mfp;
	par.vfp = vfp;
	par.n = n;
	par.K = K;
	
	for(; i < K; i++)
	{
		if(tmparray[i] != NULL){
			par.ifp = tmparray[i];
		}
		else{
			printf("There is something wrong with split files!\n");
			exit(-1);
		}
		par.i = i;
		tid = tid_array[i];
		printf("asdf\n");
		tcheck = pthread_create(&(tid), NULL, thread_start, (void *)&par);
		printf("tpid is %d\n", (int)tcheck);
		if(tcheck != 0)
		{
			printf("Thread create failed \n");
			exit(1);
		}
		else
			printf("elseee\n");
		break;
	}
	tcheck = pthread_join(tid, NULL);
	printf("you are a bold one\n");

	//REDUCER
	struct reducer_thread rt;
	pthread_t rtid;
	rt.K = K;
	rt.rfp = rfp;
	rt.n = getNumberOfLines(vfp);

	tcheck = pthread_create(&(rtid), NULL, thread_reducer, (void *)&rt);
	
	if(tcheck != 0)
	{
		printf("Thread create failed \n");
		exit(1);
	}
	printf("General Kenobi\n");
	tcheck = pthread_join(rtid, NULL);
	
	//Time
	end = clock();
	cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
	printf("time elapsed: %f\n", cpu_time_used);
	//close program
	fclose(mfp);
	fclose(vfp);
	//fclose(ifp);
	fclose(rfp);
	exit(0);
}
