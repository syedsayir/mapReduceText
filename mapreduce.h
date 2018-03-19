#include <stdio.h>
#include <stdlib.h>
#include <omp.h>
#include <mpi.h>
#include <time.h>
#include <math.h>
#include <string.h>
#include <sys/stat.h>

#define LINELEN 128
#define NUMFILES 18
#define WORDSINLINE 22
#define MAX_HASH_ENTRIES 9767

char *files[NUMFILES] = {"./RawText/39290-0.txt", 
	"./RawText/39293-0.txt", 
	"./RawText/39288.txt.utf-8.txt", 
	"./RawText/39295.txt.utf-8.txt", 
	"./RawText/39294.txt.utf-8.txt", 
	"./RawText/39297.txt.utf-8.txt", 
	"./RawText/39296.txt.utf-8.txt", 
	"./RawText/27916.txt.utf-8.txt", 
	"./RawText/2600.txt.utf-8.txt", 
	"./RawText/986.txt.utf-8.txt", 
	"./RawText/3183.txt.utf-8.txt", 
	"./RawText/34114.txt.utf-8.txt", 
	"./RawText/2638.txt.utf-8.txt", 
	"./RawText/36034.txt.utf-8.txt", 
	"./RawText/600.txt.utf-8.txt", 
	"./RawText/28054-tei.tei", 
	"./RawText/2554.txt.utf-8.txt", 
	"./RawText/1399.txt.utf-8.txt"};



typedef struct Node {
	char* line;
	struct Node* next;
} Node;

typedef struct Q {
	Node* head;
	Node* tail;
} Q;

typedef struct HTNode {
	char* word;
	int occur;
	struct HTNode* next;
} HTNode;

typedef struct HTQ {
	HTNode* head;
	HTNode* tail;
} HTQ;

char* readLine(FILE* fd);
void readFile(char* fileName);
void putWork(char* line);
Node* getWork();
void addHTNodesToQ (HTQ* htque, char* str);
HTNode* findNodeInQ (HTQ* htque, char* str);
void reducer(int idx);
void reduceHTNodeQ (HTNode* NodePtr, int idx);
char* getWord(char* line) {
	while (line) {
		if (*line == ',') {
			*line = 0;
			return line + 1;
		}
		line++;
	}
}
inline char lower(char c) { return (c>=65 && c<=90) ? c + 32 : c; }

char** lineToWords(char* line) {
	char c;
	int i = 0;
	char** buf = calloc( WORDSINLINE, (sizeof(char*)));
	char* wordStart = line;
	char* wordPtr = line;

	while( *wordPtr ) {
		*wordPtr = lower(*wordPtr);
		if ( (*wordPtr < 97 || *wordPtr > 122) && (*wordPtr != 39 || wordPtr == wordStart)) {
			// non alphablets with exception for cases of "'"
			*wordPtr = 0;
			if(wordPtr != wordStart) {
				if (*(wordPtr-1) == 39) { *(wordPtr - 1) = 0; } // getting rid of ' at endofword
				buf[i] = wordStart;
				i++;
			}
			wordStart = wordPtr+1;
		}
		wordPtr++;
	}
	if(wordPtr != wordStart) {
		buf[i] = wordStart;
		i++;
	}
	return buf;
}

unsigned int hash(const char *s) {
	union {
		unsigned long h;
		unsigned short u[8]; 
	}abc ;
	unsigned long ret;
	int i=0;
	abc.h=strlen(s);
	while (*s) { 
		abc.u[i%8] += *s + i + (*s >> ((abc.h/(i+1)) % 5)); 
		s++;
		i++;
	}
	ret = (abc.h+(abc.h>>32)); //32-bit
	return (ret % MAX_HASH_ENTRIES);
}
