#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/mman.h>
#include <dirent.h>

int main(int argc, const char **argv)
{
	if(argc < 2)
	{
		printf("Need more arguments!\n");
		exit(-1);
	}
	
	char original_path[128];
	char current_path[128];
	struct stat stats;
	DIR* dir;
	struct dirent* dirents;
	
	strcpy(original_path, argv[1]);
	strcat(original_path, "/");
	printf("original path is: %s\n", original_path);
	strcpy(current_path, original_path);
	
	dir = opendir(original_path);
	if( dir == NULL )
	{
		perror( "can't open the given path" );
		exit(-1);
	}
	else
	{
		while( (dirents = readdir(dir)) != NULL)
		{
			strcpy(current_path, original_path);
			strcat(current_path, dirents->d_name);
			if(stat(current_path, &stats) == -1)
			{
				perror("Stat");
				exit(EXIT_FAILURE);
			}
			
			printf("\nFile name:	%s\n", dirents->d_name);
			printf("inode number:	%ld\n", stats.st_ino);
			printf("file type:	");
			switch (stats.st_mode & S_IFMT) {
				case S_IFBLK:  printf("block device\n");            break;
				case S_IFCHR:  printf("character device\n");        break;
				case S_IFDIR:  printf("directory\n");               break;
				case S_IFIFO:  printf("FIFO/pipe\n");               break;
				case S_IFLNK:  printf("symlink\n");                 break;
				case S_IFREG:  printf("regular file\n");            break;
				case S_IFSOCK: printf("socket\n");                  break;
				default:       printf("unknown?\n");                break;
			}
			printf("no of blocks:	%lld\n", (long long) stats.st_blocks);
			printf("size in bytes:	%lld\n", (long long) stats.st_size);
			printf("user id:	%ld\n", (long) stats.st_uid);
		}
		
		closedir(dir);
	}
	
	return(0);
}