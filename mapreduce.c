#include "mapreduce.h"

Q* workQue;
int toRead = NUMFILES;
//omp_lock_t* qLock;
//omp_lock_t toReadLock;

HTQ** hashTables;
int abc = 0;

int flag = 0;
int threads = 4;

int rank, numP;


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
	//omp_set_lock(&toReadLock);
	//toRead--;
	//omp_unset_lock(&toReadLock);
	fclose(fd);
}

void putWork(char* line) {
	Node* node = malloc (sizeof (Node));
	static int tid = -1;
	if (flag == 1) { tid = 0; }
	else { tid = (tid+1)%threads; }
	//	omp_set_lock(&qLock[tid]);
	node->next = workQue[tid].head;
	node->line = line;
	if (workQue[tid].head == NULL) {
		workQue[tid].tail = node;
	}
	workQue[tid].head = node;
	//	omp_unset_lock(&qLock[tid]);
}

Node* getWork() {
	Node* node;
	int tid = omp_get_thread_num();
	if (flag == 1) { tid = 0; }
	//	omp_set_lock(&qLock[tid]);
	if (workQue[tid].head == NULL) {
		//omp_unset_lock(&qLock[tid]);
		return NULL;
	}
	node = workQue[tid].head;
	if (workQue[tid].head == workQue[tid].tail) {
		workQue[tid].head = workQue[tid].tail = NULL;
	}
	else {
		workQue[tid].head = node->next;
	}
	//	omp_unset_lock(&qLock[tid]);
	return node;
}

void mapper() {
	Node* lineNode;
	char* line;
	char** words;
	int i = 0;
	unsigned int hashVal;
	int tid = omp_get_thread_num();

	while ((lineNode = getWork()) != NULL) {
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
	sprintf(filename, "result_par_%d.txt", serPar, i);
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
	sprintf(filename, "result_ser.txt", serPar, i);
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
	//printf("Section READERS threadid:%d\n", omp_get_thread_num());
	exectime[0] = -omp_get_wtime();
	for (i=rank; i<NUMFILES; i+=numP) {
		//printf("READER: tid:%d total:%d\n",omp_get_thread_num(), omp_get_num_threads());
		readFile(files[i]);
	}
	exectime[0] += omp_get_wtime();
	printf("%d: Reading took: %f\n", rank, 1000*exectime[0]);


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


int main(int argc, char* argv[]) {
	int i = 0;
	int count = 0;
	FILE *fd;

	threads = atoi(argv[1]);
	workQue = malloc(threads * sizeof(Q));
	MPI_INIT(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &numP);

	//qLock = malloc(threads * sizeof(omp_lock_t));
	for (i=0; i<threads; i++) {
		//omp_init_lock( &qLock[i] );
		workQue[i].head = NULL;
		workQue[i].tail = NULL;
	}
	//omp_init_lock(&toReadLock);

	hashTables = malloc(sizeof(HTQ*) * threads);
	for (i=0; i<threads; i++) {
		hashTables[i] = calloc(MAX_HASH_ENTRIES, sizeof(HTQ));
	}
	if (rank == 0) {
	printf("Doing Parallel Execution........\n");
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
	}
	MPI_Finalize();


	return 0;

}
