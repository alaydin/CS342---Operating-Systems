#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>

FILE *createSplit(FILE *inputfileptr, int s)
{
	char line[20];
	
	FILE *intfile;
	intfile = tmpfile();

	if(intfile == NULL)
	{
		printf("Tmp file could not be created");
	}
	
	for(int i = 0; i < s; i++)
	{
		fgets(line, sizeof(line), inputfileptr);
		fputs(line, intfile);
	}
	rewind(intfile);
	return intfile;
}

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

	rewind(inputfile);
	return lineNumber;
	//End reading number of lines
}

FILE* mapperFunction(FILE* mfp, FILE* vfp, FILE* rfp)
{
	char line[20];
	char row[5];
	char column[5];
	char value[5];
	int intRow = 0;
	int intCol = 0;
	int intVal = 0;
	
	int* vectorValue;
	int n = 0;
	n = getNumberOfLines(vfp);
	vectorValue = (int*)malloc(n * sizeof(int));
	
	int resultarray[n][2];
	for(int i = 0; i < n; i++)
	{
		resultarray[i][0] = 0;
		resultarray[i][1] = 0;
	}
	
	if(vectorValue == NULL)
	{
		printf("Memory not allocated\n");
		exit(-1);
	}

	int i = 0;
	while(fgets(line, sizeof(line), vfp))
	{
		sscanf(line, "%s %s", row, value);
		if(i < n)						//To protect memory from overwriting by malloc
		{
			intVal = (int)strtol(value,(char**)NULL, 10);
			vectorValue[i] = intVal;
			i++;
		}
	}
	i = 0;
	intRow = 0;
	intCol = 0;
	intVal = 0;
	int numberOfRows = 0;
	n = getNumberOfLines(mfp);
	while(fgets(line, sizeof(line), mfp))
	{
		sscanf(line, "%s %s %s", row, column, value);
		
		if(i < n)
		{
			intRow = (int)strtol(row,     (char**)NULL,10) - 1;
			intCol = (int)strtol(column,  (char**)NULL,10) - 1;
			intVal = (int)strtol(value,   (char**)NULL,10);
			if(intVal != 0)
			{
				resultarray[intRow][0] = intRow + 1;
				resultarray[intRow][1] = resultarray[intRow][1] + (intVal * vectorValue[intCol]);
			}
			if(resultarray[intRow][0] > numberOfRows)
				numberOfRows = resultarray[intRow][0];
		}
		i++;
	}
	
	//This part writes resultarray into the file
	char convert[20];
	for(int i = 0; i < numberOfRows; i++)
	{
		if(resultarray[i][0] != 0)
		{
			sprintf(convert, "%d %d\n", resultarray[i][0], resultarray[i][1]);
			fputs(convert, rfp);
		}
	}
	rewind(mfp);
	rewind(vfp);
	rewind(rfp);
	free(vectorValue);
	return rfp;
}

void sumFiles(FILE* rfp, char **numbers, int n, int K)
{
	FILE* reader;
	int rowNumber = 1;
	int rowValue = 0;
	char line[20];
	char row[5];
	char value[5];
	int intRow = 0;
	int intValue = 0;
	for(int i = 0; i < n; i++)
	{
		for(int j = 0; j < K; j++)
		{
			reader = fopen(numbers[j], "r");
			while(fgets(line, sizeof(line), reader))
			{
				sscanf(line, "%s %s", row, value);
				intRow   = (int)strtol(row,(char**)NULL,10);
				intValue = (int)strtol(value,(char**)NULL,10);
				if(intRow == rowNumber)
				{
					rowValue = rowValue + intValue;
					break;
				}
				else if(intRow > rowNumber)
				{
					break;
				}
			}
		}
		sprintf(line, "%d %d\n", rowNumber, rowValue);
		printf("%s\n",line);
		fputs(line, rfp);
		rowNumber++;
		rowValue = 0;
	}
	
	fclose(reader);
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
	FILE *mfp, *vfp, *rfp, *ifp;
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
	
	//Clock star
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
	
	//Intermediate files
	char *numbers[10] = {"intfile0.txt","intfile1.txt","intfile2.txt","intfile3.txt","intfile4.txt"
								,"intfile5.txt","intfile6.txt","intfile7.txt","intfile8.txt","intfile9.txt"};
	for(int i = 0; i < K; i++)
	{
		ifp = fopen(numbers[i], "w");
	}
	
	//File* array to keep tmpfiles
	FILE *tmparray[K];
	/*This part checks whether number of ceilings (s) is 0 or not. If it is 0 gives error message and returns -1.
	 *Creates the splits.
	 *Stores the resulting files from each split into an array of file pointers
	 */
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
		printf("There is something wrong with matrix size or split (K) value!\n");
		exit(-1);
	}
	//End of the part
	
	//Create child processes
	pid_t child_pid, wpid;
	int status;
	int i = 0;
	for(; i < K; i++)
	{
		if(tmparray[i] != NULL){
			ifp = tmparray[i];
		}
		else{
			printf("There is something wrong with split files!\n");
			exit(-1);
		}

		if((child_pid = fork() ) == 0)
		{
			break;
		}
	}
	while ((wpid = wait(&status)) > 0);
	
	if(child_pid == 0)
	{
		fclose(mfp);
		fclose(vfp);
		fclose(rfp);
		
		//Vector multipication
		mfp = fopen(argv[1],"r");
		vfp = fopen(argv[2],"r");
		rfp = fopen(numbers[i],"w");
		mapperFunction(ifp, vfp, rfp);

		//free(resultarray);
		fclose(mfp);
		fclose(vfp);
		fclose(rfp);
		exit(0);
	}
	
	//Reducer
	pid_t wpid2;
	child_pid = fork();
	while ((wpid2 = wait(&status)) > 0);
	if(child_pid == 0)
	{
		mfp = fopen(argv[1],"r");
		vfp = fopen(argv[2],"r");
		rfp = fopen(argv[3],"w");
		int n = getNumberOfLines(vfp);
		sumFiles(rfp,numbers,n,K);
		
		fclose(mfp);
		fclose(vfp);
		fclose(rfp);
		exit(0);
	}
	
	//Time
	end = clock();
	cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
	printf("time elapsed: %f\n", cpu_time_used);
	//close program
	fclose(mfp);
	fclose(vfp);
	fclose(ifp);
	fclose(rfp);
	exit(0);
}
