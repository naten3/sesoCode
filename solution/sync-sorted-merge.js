"use strict";

// Print all entries, across all of the sources, in chronological order.

/**
 * Use a min-heap to track the oldest item on each logSource, and get the oldest of all of those
 */
module.exports = (logSources, printer) => {

  const heap = new MinHeap(heapItem => heapItem[0].date);

  for (const logSource of logSources ) {

    const top = logSource.pop();
    if(top !== false) {
       heap.push([top, logSource])
    }
  }

  while(heap.length()) {
    const [top, logSource] = heap.pop();
    printer.print(top);

    const next = logSource.pop();
    if(!next !== false) {
      heap.push([next, logSource])
    }

  }

  return console.log("Sync sort complete.");
};

  class MinHeap {
    getCompValueFn;
    heap = [];

    constructor(getCompValueFn){
      this.getCompValueFn = getCompValueFn;
    }
     

    length() {
      return this.heap.length;
    }

    pop() {
      const heap = this.heap
      if(!heap.length) {
        return null
      }

      [heap[0], heap[heap.length-1]] = [ heap[heap.length-1], heap[0]];
      const top =  heap.pop();

      //reheapify
      let curr = 0;
      while (2*curr + 1  < heap.length) {
         const leftChild = 2*curr+1;
         const rightChild = 2*curr+2;
         // find smallest child
         const minChild = (rightChild < heap.length && this.getCompValueFn(heap[rightChild]) < this.getCompValueFn(heap[leftChild]) ) ? rightChild :leftChild;

         // if smallest child is smaller than curr swap, else we have a heap
         if(this.getCompValueFn(heap[minChild]) < this.getCompValueFn(heap[curr])){
          [heap[minChild], heap[curr]] = [heap[curr], heap[minChild]];
          curr = minChild
        } else {
          break
        }
      }

      return top
    }
    
    push(key) {
      const heap = this.heap;
      heap.push(key);
      let curr = heap.length - 1;

      while(curr > 0){
        let parent = Math.floor((curr-1)/2)
        if( this.getCompValueFn(heap[curr]) < this.getCompValueFn(heap[parent]) ){
          [ heap[curr], heap[parent] ] = [ heap[parent], heap[curr] ]
          curr = parent
        } else{
          // if no swap, break, since we heap is stable now
          break
        }
      } 
    }
  }