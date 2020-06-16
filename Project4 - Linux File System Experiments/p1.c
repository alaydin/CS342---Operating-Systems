#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/mman.h>

#define BLOCKSIZE 4094
#define DISKNAME "vdisk1.bin"

int vdisk_fd;

int create_vdisk (char *vdiskname, int N)
{
	char command[BLOCKSIZE]; 
	 
	sprintf(command, "dd if=/dev/zero of=%s bs=%d count=%d", vdiskname, BLOCKSIZE, N);
	printf ("executing command = %s\n", command); 
	system (command); 
	return (0); 
}

int main(int argc, char **argv)
{
	if(argc < 2)
	{
		printf("Need more arguments!\n");
		exit(-1);
	}
	
	unlink(DISKNAME);
	
	int* arr;
	int blocknumber = 0;
	int offset;
	int n;
	int N;
	int ret;
	
	N = (int)strtol(argv[1],(char**)NULL,10);
	arr = (int*) calloc (1024, sizeof(int));
	
	ret  = create_vdisk (DISKNAME, N);
	if (ret != 0)
	{
		printf ("there was an error in creating the disk\n");
		exit(1); 
	}
	vdisk_fd = open(DISKNAME, O_RDWR);
	if(vdisk_fd == -1)
	{
		printf("can't open file!\n");
		exit(-1);
	}
	
	for(int i = 0; i < N; i++)
	{
		offset = blocknumber * BLOCKSIZE;
		lseek(vdisk_fd, (off_t) offset, SEEK_SET);
		n = write(vdisk_fd, arr, BLOCKSIZE);
		if (n != BLOCKSIZE)
		{
			printf ("write error\n");
			return (-1);
		}
		blocknumber++;
	}
	
	free(arr);
	close(vdisk_fd);
	return(0);
}