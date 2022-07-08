#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hash_file.h"


#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}


HT_ErrorCode HT_Init() {
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int buckets) {
  int fileDesc;
  BF_Block* firstBlock;
  CALL_BF(BF_CreateFile(filename));
  CALL_BF(BF_OpenFile(filename, &fileDesc));

  BF_Block_Init(&firstBlock);
  CALL_BF(BF_AllocateBlock(fileDesc,firstBlock));
  char* data = BF_Block_GetData(firstBlock);
  *data = '$';
  char* numBuckets = data+1;
  *numBuckets = (char)buckets;
  BF_Block_SetDirty(firstBlock);
  CALL_BF(BF_UnpinBlock(firstBlock));
  BF_Block_Destroy(&firstBlock);

  int i;
  for(i = 0; i < buckets; i++){
    BF_Block* mapBlock;
    BF_Block_Init(&mapBlock);
    CALL_BF(BF_AllocateBlock(fileDesc,mapBlock));
    char* data = BF_Block_GetData(mapBlock);
    *data = 0;
    BF_Block_SetDirty(mapBlock);
    CALL_BF(BF_UnpinBlock(mapBlock));
    BF_Block_Destroy(&mapBlock);
  }
  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
  BF_Block *block;
  int i;
  for(i = 0; i < MAX_OPEN_FILES; i++){
    if(OpenFiles[i] == 0){
      break;
    }
  }

  if(i == MAX_OPEN_FILES){
    printf("Opened more than 20 files ~ABORT PROGRAM - FATAL ERROR~\n");
    return HT_ERROR;
  }

  BF_Block_Init(&block);
  CALL_BF(BF_OpenFile(fileName, indexDesc));
  CALL_BF(BF_GetBlock(*indexDesc, 0, block));

  char* data = BF_Block_GetData(block);

  if(*data != '$'){
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);
    return HT_ERROR;
  }

  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);

  OpenFiles[i] = *indexDesc;

  return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc) {

  CALL_BF(BF_CloseFile(indexDesc));

  int i;
  for(i = 0; i < MAX_OPEN_FILES; i++){
    if(OpenFiles[i] == indexDesc){
      OpenFiles[i] = 0;
      break;
    }
  }

  return HT_OK;
}

int blockNumber = BUCKETS_NUM;


HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
  BF_Block *block;
  BF_Block *bucketBlock;

  int numberOfRecords = (BF_BLOCK_SIZE - HEADER_SIZE) / sizeof(record);

  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(indexDesc, 0, block));

  char* data = BF_Block_GetData(block);
  int buckets = *(data + 1);
  int toBucket = record.id % buckets; //HASHING

  BF_Block_Init(&bucketBlock);
  CALL_BF(BF_GetBlock(indexDesc, toBucket+1, bucketBlock));
  unsigned char* bucketData = BF_Block_GetData(bucketBlock); // number of blocks in certain bucket

  if(*(bucketData) == 0){
    char temp = *(bucketData);
    *(bucketData) = temp + 1;
    blockNumber++;
    memcpy(bucketData+1, &blockNumber, sizeof(int));  //first Block of bucket
    BF_Block_SetDirty(bucketBlock);
    CALL_BF(BF_UnpinBlock(block));
    CALL_BF(BF_AllocateBlock(indexDesc, block));


    char* blockHeaderData = BF_Block_GetData(block);
    *blockHeaderData = 1; // number of records

    unsigned int nextBlock;

    nextBlock = 0; //default value for next block (pseudo null)


    memcpy(blockHeaderData+1, &nextBlock, sizeof(int));


    memcpy((blockHeaderData+HEADER_SIZE), &record, sizeof(record));

  }else{

    unsigned int nextBlock = *(bucketData+1);
    unsigned char* blockHeaderData;
    while(nextBlock != 0){
      CALL_BF(BF_UnpinBlock(block));
      CALL_BF(BF_GetBlock(indexDesc, nextBlock,block));
      blockHeaderData = BF_Block_GetData(block);
      memcpy(&nextBlock, blockHeaderData+1, sizeof(int));


    }
    if(*(blockHeaderData) != numberOfRecords){
      memcpy((blockHeaderData + ((*blockHeaderData) * sizeof(record)) + HEADER_SIZE) , &record, sizeof(record));
      *(blockHeaderData) = *(blockHeaderData) + 1;

    }else{
      blockNumber++;
      *(blockHeaderData+1) = blockNumber;
      int num;

      BF_Block_SetDirty(block);
      CALL_BF(BF_UnpinBlock(block));

      CALL_BF(BF_AllocateBlock(indexDesc, block));

      char* blockHeaderData = BF_Block_GetData(block);
      *blockHeaderData = 1; // number of records
      unsigned int nextBlock;
      nextBlock = 0; //default value for next block (pseudo null)
      memcpy(blockHeaderData+1, &nextBlock, sizeof(int));


      memcpy((blockHeaderData+HEADER_SIZE), &record, sizeof(record));
    }
  }

  CALL_BF(BF_UnpinBlock(bucketBlock));
  BF_Block_Destroy(&bucketBlock);


  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);

  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {
  BF_Block* block;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(indexDesc, 0, block));
  Record* record;

  char* data = BF_Block_GetData(block);
  int buckets = *(data + 1);
  CALL_BF(BF_UnpinBlock(block));
  int found = 0;


  if(id == NULL){
    int i;
    for(i = 0; i < buckets; i++){
      CALL_BF(BF_GetBlock(indexDesc, i+1, block));
      char* data = BF_Block_GetData(block);
      unsigned int nextBlock, next;
      memcpy(&nextBlock, data+1, sizeof(int));

      char* blockHeaderData;
      do{
        CALL_BF(BF_UnpinBlock(block));
        CALL_BF(BF_GetBlock(indexDesc,nextBlock,block));
        blockHeaderData = BF_Block_GetData(block);
        memcpy(&next, blockHeaderData+1, sizeof(int));
        int numberOfRecords = *(blockHeaderData);
        nextBlock = next;
        int j;
        for(j = 0; j < numberOfRecords; j++){
          record = (Record*) (blockHeaderData + HEADER_SIZE + j*sizeof(Record));
          if(strcmp(record->name, " ") == 1){
            printf("Id: %d | Name: %s | Surname: %s | City: %s \n",record->id, record->name, record->surname, record->city);
          }
        }
      }while(next != 0);
    }
  }else{
    int toBucket = *(id) % buckets;

    CALL_BF(BF_GetBlock(indexDesc, toBucket+1, block));
    char* bucketData = BF_Block_GetData(block);
    CALL_BF(BF_UnpinBlock(block));

    unsigned int nextBlock;
    memcpy(&nextBlock, bucketData+1, sizeof(int));

    CALL_BF(BF_GetBlock(indexDesc, nextBlock , block)); //goes to first block of bucket


    unsigned int next;
    char* blockHeaderData;
    int i;
    do{
      CALL_BF(BF_UnpinBlock(block));
      CALL_BF(BF_GetBlock(indexDesc,nextBlock,block));
      blockHeaderData = BF_Block_GetData(block);
      memcpy(&next, blockHeaderData+1, sizeof(int));
      int numberOfRecords = *(blockHeaderData);
      nextBlock = next;
      for(i = 0; i < numberOfRecords; i++){
        record = (Record*) (blockHeaderData + HEADER_SIZE + i*sizeof(Record));
        if(record->id == *id){
          found = 1;
          printf("Id: %d | Name: %s | Surname: %s | City: %s \n",record->id, record->name, record->surname, record->city);
          break;
        }
      }
    }while(next != 0);
    if(found == 0){
      printf("ID DOES NOT EXIST\n");
    }
  }


  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  return HT_OK;
}

HT_ErrorCode HT_DeleteEntry(int indexDesc, int id) {

  BF_Block* block;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(indexDesc, 0, block));
  Record* record;
  Record* wanted_record;
  int count;
  CALL_BF(BF_GetBlockCounter(indexDesc, &count));
  char* data = BF_Block_GetData(block);

  BF_Block* wanted_block;
  BF_Block* bucket_block;

  int buckets = *(data + 1);
  CALL_BF(BF_UnpinBlock(block));
  int toBucket = id % buckets;
  BF_Block_Init(&bucket_block);
  CALL_BF(BF_GetBlock(indexDesc, toBucket+1, bucket_block));
  char* bucketData = BF_Block_GetData(bucket_block);

  unsigned int nextBlock;

  memcpy(&nextBlock, bucketData+1, sizeof(int));

  CALL_BF(BF_GetBlock(indexDesc, nextBlock , block)); //goes to first block of bucket

  unsigned int next;
  unsigned int prev;
  char* blockHeaderData;
  int i;
  BF_Block_Init(&wanted_block);

  do{
    prev = nextBlock; //new is now old
    CALL_BF(BF_UnpinBlock(block));
    CALL_BF(BF_GetBlock(indexDesc,nextBlock,block));
    blockHeaderData = BF_Block_GetData(block);
    memcpy(&next, blockHeaderData+1, sizeof(int)); //next is the number of a new block
    int numberOfRecords = *(blockHeaderData);
    nextBlock = next; // nextBlock loses its number of the current block
    for(i = 0; i < numberOfRecords; i++){
      record = (Record*) (blockHeaderData + HEADER_SIZE + i*sizeof(Record));
      if(record->id == id){
        wanted_record = record;
        CALL_BF(BF_GetBlock(indexDesc, prev, wanted_block)); // prev has the number of its next block, which is the current block
        break;
      }
    }
  }while(next != 0);

  int numberOfRecords;
  memcpy(&numberOfRecords,blockHeaderData,sizeof(int));
  Record* lastRecord = (Record*) (blockHeaderData + HEADER_SIZE + numberOfRecords*sizeof(Record));////

  memcpy(wanted_record,lastRecord,sizeof(Record));
  BF_Block_SetDirty(wanted_block);
  CALL_BF(BF_UnpinBlock(wanted_block));
  *blockHeaderData = *blockHeaderData - 1;
  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));

  if(*blockHeaderData == 0){


    CALL_BF(BF_GetBlock(indexDesc,prev, block));
    data = BF_Block_GetData(block);
    memcpy(&nextBlock, data+1, sizeof(int));
    nextBlock = 0;
    unsigned int firstBlock;
    memcpy(&firstBlock, bucketData+1, sizeof(int));
    if(firstBlock == prev){
      *bucketData = *bucketData-1;
    }
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_SetDirty(bucket_block);

  }

  BF_Block_Destroy(&wanted_block);
  CALL_BF(BF_UnpinBlock(bucket_block));

  BF_Block_Destroy(&bucket_block);

  BF_Block_Destroy(&block);

  return HT_OK;
}
