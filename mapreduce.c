#include "mapreduce.h"

Q* workQue;
int toRead;
//omp_lock_t* qLock;
//omp_lock_t toReadLock;

HTQ** hashTables;
int abc = 0;

int flag = 0;
int threads = 4;

int rank, numP;
int* filesToReadRank;


char* readLine(FILE* fd) {
	char* buf = malloc(LINELEN);
	return fgets ( buf, LINELEN, fd );
}

void readFile(char* fileName) {
	char* line;
	FILE* fd = fopen(fileName, "r");
	if (fd == NULL) {
		printf("Cant open FILE: %s.\n",fileName);
		return;
	}
	//printf("%s\n",fileName);
	while ((line = readLine(fd)) != NULL) {
		putWork (line);
	}
//	omp_set_lock(&toReadLock);
	toRead--;
//	omp_unset_lock(&toReadLock);
	fclose(fd);
}

void putWork(char* line) {
	Node* node = malloc (sizeof (Node));
	static int tid = -1;
	if (flag == 1) { tid = 0; }
	else { tid = (tid+1)%threads; }
//		omp_set_lock(&qLock[tid]);
	node->next = workQue[tid].head;
	node->line = line;
	if (workQue[tid].head == NULL) {
		workQue[tid].tail = node;
	}
	workQue[tid].head = node;
//		omp_unset_lock(&qLock[tid]);
}

Node* getWork() {
	Node* node;
	int tid = omp_get_thread_num();
	if (flag == 1) { tid = 0; }
	if (workQue[tid].head == NULL) {
		return NULL;
	}
//	omp_set_lock(&qLock[tid]);
	node = workQue[tid].head;
	if (workQue[tid].head == workQue[tid].tail) {
		workQue[tid].head = workQue[tid].tail = NULL;
	}
	else {
		workQue[tid].head = node->next;
	}
//		omp_unset_lock(&qLock[tid]);
	return node;
}

void mapper() {
	Node* lineNode;
	char* line;
	char** words;
	int i = 0;
	unsigned int hashVal;
	int tid = omp_get_thread_num();

	while ((lineNode = getWork()) != NULL || toRead > 0) {
		if (lineNode == NULL) {
			abc++;
			continue;
		}
		line = lineNode->line;
		words = lineToWords(line);
		i = 0;
		while (words[i]) {
			hashVal = hash(words[i]);
			addHTNodesToQ( &(hashTables[tid][hashVal]), words[i] );
			i++;
		}
	}
}

void addHTNodesToQ (HTQ* htque, char* str) {
	HTNode* htnode;
	if ( (htnode = findNodeInQ(htque, str)) != NULL ) {
		htnode->occur++;	
		return;
	}
	htnode = malloc(sizeof(HTNode));
	htnode->word = str;
	htnode->occur = 1;
	htnode->next = htque->head;
	if (htque->head == NULL) {
		htque->tail = htnode;
	}
	htque->head = htnode;
	return;
}

void reduceHTNodeQ (HTNode* NodePtr, int idx) {
	HTNode* currNodePtr;
	HTQ* htque = &hashTables[0][idx];
	char* str = NodePtr->word;
	HTNode* htnode;
	if ( (htnode = findNodeInQ(htque, str)) != NULL ) {
		htnode->occur += NodePtr->occur;

		//printf("RE:%s %d \n",str,htnode->occur );
		return;
	}
	htnode = NodePtr;
	htnode->next = htque->head;
	if (htque->head == NULL) {
		htque->tail = htnode;
	}
	htque->head = htnode;
	return;
}

HTNode* findNodeInQ (HTQ* htque, char* str) {
	HTNode* nodePtr = htque->head;
	while ( nodePtr ) {
		if ( strcmp(nodePtr->word, str) == 0 ) {
			return nodePtr;
		}
		nodePtr = nodePtr->next;
	}
	return NULL;
}

void reducer(int idx) {
	int i;
	HTNode* NodePtr;
	HTNode* currNodePtr;


	for (i=1; i<threads; i++) {
		NodePtr = hashTables[i][idx].head;
		while ( NodePtr ) {
			currNodePtr = NodePtr;
			NodePtr = NodePtr->next;
			reduceHTNodeQ (currNodePtr, idx);
		}
	}
}

void csvReducer() {
	int i;
	HTNode* NodePtr;
	FILE* fd;
	int idx;
	char filename[20];
	char* word;
	char *buf=malloc(50);
	for(i=1; i<numP; i++){
		sprintf(filename, "intmed_%d.txt", i);
		fd = fopen (filename, "r");
		while (fgets (buf, 50, fd) != NULL) {
			HTNode* currNodePtr = malloc(sizeof(HTNode));
			word = getWord(buf);
			idx = atoi(buf);
			currNodePtr->word = word;
			word = getWord(word);
			getWord(word);
			currNodePtr->occur = atoi(word);
			//printf("%s: %d, %s, %d. ROOT\n",filename, idx, currNodePtr->word, currNodePtr->occur);
			reduceHTNodeQ (currNodePtr, idx);
			buf=malloc(50);
		}
		fclose(fd);
	}
}

void parWriter(int i) { 
	FILE* fd;
	int j;
	int max;
	char filename[20];
	HTNode* nodePtr;
	sprintf(filename, "result_par_%d.txt", i);
	fd = fopen (filename, "w");
	j = i*(MAX_HASH_ENTRIES/threads);
	max = (i==threads-1) ? MAX_HASH_ENTRIES : (j + MAX_HASH_ENTRIES/threads);
	for (j; j<max; j++) {
		nodePtr = hashTables[0][j].head;
		while(nodePtr) {
			fprintf(fd, "%-20s%d\n", nodePtr->word, nodePtr->occur);
			nodePtr = nodePtr->next;
		}
	}
	fclose(fd);
}

void serWriter() { 
	FILE* fd;
	int j;
	int max;
	char filename[20];
	HTNode* nodePtr;
	sprintf(filename, "result_ser.txt");
	fd = fopen (filename, "w");
	j = 0;
	for (j; j<MAX_HASH_ENTRIES; j++) {
		nodePtr = hashTables[0][j].head;
		while(nodePtr) {
			fprintf(fd, "%-20s%d\n", nodePtr->word, nodePtr->occur);
			nodePtr = nodePtr->next;
		}
	}
	fclose(fd);
}

void csvWriter() { 
	FILE* fd;
	int j;
	char filename[20];
	HTNode* nodePtr;
	sprintf(filename, "intmed_%d.txt", rank);
	fd = fopen (filename, "w");
	for (j=0; j<MAX_HASH_ENTRIES; j++) {
		nodePtr = hashTables[0][j].head;
		while(nodePtr) {
			fprintf(fd, "%d,%s,%d,\n", j, nodePtr->word, nodePtr->occur);
			nodePtr = nodePtr->next;
		}
	}
	fclose(fd);
}

void parallelExec() {
	int i, j;
	double exectime[4];
	omp_set_nested(1);
	


#pragma omp sections
{
#pragma omp section
{
	//printf("Section READERS threadid:%d\n", omp_get_thread_num());
	toRead = filesToReadRank[0];
	exectime[0] = -omp_get_wtime();
	for (i=0; i<filesToReadRank[0]; i++) {
		//printf("READER: tid:%d total:%d\n",omp_get_thread_num(), omp_get_num_threads());
		readFile(files[filesToReadRank[i+2]]);
	}
	exectime[0] += omp_get_wtime();
	printf("%d: Reading took: %f\n", rank, 1000*exectime[0]);
}

#pragma omp section
{
	//printf("Section MAPPERS threadid:%d\n", omp_get_thread_num());
	exectime[1] = -omp_get_wtime();
	//sleep(1);
#pragma omp parallel num_threads(threads)
	{
		//printf("MAPPER: mytid is: %d\n",omp_get_thread_num());
		mapper();
	}
	exectime[1] += omp_get_wtime();
	printf("%d: Mapping took: %f\n", rank, 1000*exectime[1]);
}
}

	exectime[2] = -omp_get_wtime();
#pragma omp parallel for num_threads(threads)
	for (i=0; i<MAX_HASH_ENTRIES; i++) {
		reducer(i);
	}
	exectime[2] += omp_get_wtime();
	printf("%d: Reduction took: %f\n", rank, 1000*exectime[2]);


	if(rank) {
		csvWriter();
	}
	MPI_Barrier(MPI_COMM_WORLD);
	if (rank == 0) {
		csvReducer();
		exectime[3] = -omp_get_wtime();
		//#pragma omp parallel for num_threads(threads)
		for (i=0; i<threads; i++) {
			parWriter(i);
		}
		exectime[3] += omp_get_wtime();
		printf("Writing took: %f\n", 1000*exectime[3]);
	}
}


void serialExec() {
	int i;
	double exectime;

	exectime = -omp_get_wtime();
	for (i=0; i< NUMFILES; i++) {
		readFile(files[i]);
	}
	exectime += omp_get_wtime();
	printf("%d: Reading took: %f\n", rank, 1000*exectime);


	exectime = -omp_get_wtime();
	mapper();
	exectime += omp_get_wtime();
	printf("%d: Mapping took: %f\n", rank, 1000*exectime);


	exectime = -omp_get_wtime();
	for (i=0; i<threads; i++) {
		serWriter();
	}
	exectime += omp_get_wtime();
	printf("%d: Writing took: %f\n", rank, 1000*exectime);
}


int getFileSizes(int** fileSizes) {
	struct stat st;
	int i;
	int totalSize = 0;
	for (i=0; i<NUMFILES; i++) {
		stat(files[i], &st);
		fileSizes[i][0] = i;
		fileSizes[i][1] = st.st_size;
		totalSize += fileSizes[i][1];
	}
	return totalSize;
}

void sortFileSizes(int** fileSizes) {
	int tmp;
	int i, j;
	for (i=0; i<NUMFILES; i++) {
		int max = fileSizes[i][1];
		int maxIdx = i;
		for (j=i; j<NUMFILES; j++) {
			if (fileSizes[j][1]> max) {
				max = fileSizes[j][1];
				maxIdx = j;
			}
		}
		tmp = fileSizes[maxIdx][0];
		fileSizes[maxIdx][0] = fileSizes[i][0];
		fileSizes[i][0] = tmp;
		tmp = fileSizes[maxIdx][1];
		fileSizes[maxIdx][1] = fileSizes[i][1];
		fileSizes[i][1] = tmp;
	}
}
void getBalLoad(int** filesToRead, int** fileSizes){
	int i, j;
	int cnt, tmp;
	int avgSize = getFileSizes(fileSizes)/numP;
	sortFileSizes(fileSizes);
	int direction = 1;

	for (i=0; i<numP; i++) {
		for(j=0;j<NUMFILES-numP+3;j++)
			filesToRead[i][j] = 0; //total Files to be read
	}
	for (i=0; i<numP; i++) {
		filesToRead[i][1] += fileSizes[i][1];
		filesToRead[i][2+filesToRead[i][0]++] = fileSizes[i][0];
	}
	j = numP - 1;
	for (i=numP; i<NUMFILES; i++) {
		cnt=0;
		while(fileSizes[i][1] + filesToRead[j][1] > avgSize) {
			cnt++;
			if(cnt==numP){
				j=rand()%numP;
				break;
			}

			if (direction == 0) {
				j++;
				if(j==numP){
					direction =1;
					j--;
				}
			}
			else {
				j--;
				if(j==-1) {
					direction = 0;
					j++;
				}
			}
		}

		filesToRead[j][1] += fileSizes[i][1];
		tmp = filesToRead[j][0]++;
		filesToRead[j][2+tmp] = fileSizes[i][0];
		if (direction == 0) {
			j++;
			if (j == numP) {
				j--;
				direction = 1;
			}
		}
		else {
			j--;
			if (j == -1) {
				j++;
				direction = 0;
			}
		}
	}
	/* PRINT FOR CHECK
	   for (i=0; i<numP; i++) {
	   for (j=0; j<2+filesToRead[i][0]; j++){
	   printf("%9d ", filesToRead[i][j]);
	   }
	   printf("\n");
	   }
	   */
}


int main(int argc, char* argv[]) {
	int i, j;
	int count = 0;
	FILE *fd;
	int tmp;
	char filename[20];

	threads = atoi(argv[1]);
	workQue = malloc(threads * sizeof(Q));
	MPI_INIT(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &numP);
	filesToReadRank = malloc(sizeof(int) * (NUMFILES-numP+3));
//	qLock = malloc(threads * sizeof(omp_lock_t));
	for (i=0; i<threads; i++) {
//		omp_init_lock( &qLock[i] );
		workQue[i].head = NULL;
		workQue[i].tail = NULL;
	}
//	omp_init_lock(&toReadLock);

	hashTables = malloc(sizeof(HTQ*) * threads);
	for (i=0; i<threads; i++) {
		hashTables[i] = calloc(MAX_HASH_ENTRIES, sizeof(HTQ));
	}
	if (rank == 0) {

		int** fileSizes = malloc(NUMFILES * sizeof(int*));
		for (i=0; i<NUMFILES; i++) {
			fileSizes[i] = malloc(2 * sizeof(int));
		}
		int** filesToRead = malloc(sizeof(int*) * numP);
		for (i=0; i<numP; i++) {
			filesToRead[i] = malloc(sizeof(int) * (NUMFILES-numP+3));
		}
		getBalLoad(filesToRead, fileSizes);
		printf("Doing Parallel Execution........\n");
		for (i=1; i<numP; i++) {
			MPI_Send(	(void*) filesToRead[i],
					NUMFILES-numP+3,
					MPI_INT,
					i,
					123+i,
					MPI_COMM_WORLD);
		}
		for (i=0; i<NUMFILES-numP+3; i++) {
			filesToReadRank[i] = filesToRead[0][i];
		}
	}
	else {
		MPI_Recv(	(void*) filesToReadRank,
				NUMFILES-numP+3,
				MPI_INT,
				0,
				123+rank,
				MPI_COMM_WORLD,
				MPI_STATUS_IGNORE);
	}


	double exectime1 = -MPI_Wtime();
	parallelExec();
	exectime1 += MPI_Wtime();

	hashTables = malloc(sizeof(HTQ*));
	hashTables[0] = calloc(MAX_HASH_ENTRIES, sizeof(HTQ));


	flag = 1;
	if (rank == 0) {
		printf("moving to sequential........\n");
		double exectime2 = -MPI_Wtime();
		serialExec();
		exectime2 += MPI_Wtime();
		printf("Parallel Time: %f, Sequential Time: %f, Speedup: %f\n", exectime1, exectime2, exectime2/exectime1);
		system("cat result_par_*.txt|sort>par.txt");
		system("cat result_ser.txt|sort>ser.txt");
		system("rm result_par_*.txt; rm result_ser.txt");
		system("rm intmed_*.txt");
		printf("\nChecksums for Ser & Par Version Files\n");
		system("cksum ser.txt; cksum par.txt");

	}
	MPI_Finalize();


	return 0;

}
/*
	int avgSize = getFileSizes(fileSizes)/numP;
	sortFileSizes(fileSizes);
	int filesToRead[numP][NUMFILES-numP+2];
	for (i=0; i<numP; i++) {
		filesToRead[i][0] = 0; //total Files to be read
		filesToRead[i][1] = 0; //total size to be read
	}
	printf("%d\n", avgSize);
	printf("%d\n", fileSizes[0][1]);
	
	int direction = 1;
	for (i=0; i<numP; i++) {
		filesToRead[i][1] += fileSizes[i][1];
		filesToRead[i][2 + filesToRead[i][0]++] = fileSizes[i][0];
	}
	j = numP - 1;
	for (i=numP; i<NUMFILES; i++) {
		while(fileSizes[i][1] + filesToRead[j][1] > avgSize) {
			printf("_%d\n",j);
			if (direction == 0)
				j++;
			else
				j--;
		}
		filesToRead[j][1] += fileSizes[i][1];
		printf("assigned %d\n", j);
		tmp = filesToRead[j][0]++;
		printf("- %d\n",tmp);
		filesToRead[j][2+tmp] = fileSizes[i][0];
		if (direction == 0) {
			j++;
			if (j == numP) {
				j--;
				direction = 1;
			}
		}
		else {
			j--;
			if (j == -1) {
				j++;
				direction = 0;
			}
		}
	}

	for (i=0; i<numP; i++) {
		for (j=0; j< NUMFILES-numP+2; j++){
			printf("%6d ", filesToRead[i][j]);
		}
		printf("\n");
	}


	//for (i=0; i<NUMFILES; i++) {
	//	printf("%d, %d\n",fileSizes[i][0], fileSizes[i][1]);
	//}
	//
	*/
