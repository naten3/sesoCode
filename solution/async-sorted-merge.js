"use strict";

// Print all entries, across all of the *async* sources, in chronological order.

/**
 *  The async case isn't that different from the sync case. We can still use a heap, and we still have to
 *  wait to see the top item from each log source before we can print, which can take a while.
 * We can mitigate the delay by using a buffer. I see we don't like OOP but I think having a class
 * to wrap the logSource and encapuslate the buffering logic makes sense.
 */
module.exports = (logSources, printer) => {
  return print(logSources, printer).then((r) =>
    console.log("Async sort complete.")
  );
};

async function print(logSources, printer) {
  const heap = new AsyncMinHeap(
    async (bufferedLogSource) => (await bufferedLogSource.peek()).date
  );

  // first wrap all so we wait for all the logs at once.
  // Space is cheap so I'm using a big buffer, tune this to your use case/ expected number of log soures
  const bufferedLogSources = logSources.map(
    (l) => new BufferedLogSource(l, 50)
  );

  for (let bls of bufferedLogSources) {
    await heap.push(bls);
  }

  while (heap.length()) {
    const next = await heap.pop();
    const value = await next.pop();
    if (value !== false) {
      printer.print(value);
      await heap.push(next);
    }
  }
}

/**
 * Holds a buffered log item.
 * Basically used to get a replayable promise
 */
class BufferedLogItem {
  promiseFn;
  promise = null;
  value = null;
  onComplete;
  

  constructor(promiseFn, onComplete) {
    this.promiseFn = promiseFn;
    this.onComplete = onComplete;
  }

  fetch() {
    this.promise = this.promiseFn();
    this.promise.then(p => {
      this.value = p;
      this.onComplete();
    })
  }

  // we can't call getValue before fetch is called, 
  // but this shouldn't happen because we only ever want the value on the top item,
  // which we should always have fetched or be fetching.
  async getValue() {
    if (this.value !== null) {
      return this.value;
    }
    this.value = await this.promise;
    return this.value;
  }
}

/**
 * Wraps a log source, but proactively fetches {bufferSize} items.
 * logSource.popAsync() can only be called sequentially for a given log source,
 * so we request a new item 
 * - when a new item is added to the buffer
 * - when a previous fetch completes
 * As long as we aren't currently requesting already.
 */
class BufferedLogSource {
  logSource;
  bufferSize;
  buffer = [];
  isFetching = false;

  constructor(logSource, bufferSize = 5) {
    this.logSource = logSource;
    this.bufferSize = bufferSize;

    // populate buffer and request logs ahead of time
    for (let i = 0; i < bufferSize; i++) {
      this.putNextItem();
    }
  }

  async pop() {;
    if(!this.buffer.length) {
      return false;
    }
    const topItem = this.buffer.shift();
    this.putNextItem();
    return await topItem.getValue();
  }

  async peek() {
    if(!this.buffer.length) {
      return false;
    }
    return await this.buffer[0].getValue();
  }

  putNextItem() {
    const item = new BufferedLogItem(() => this.logSource.popAsync(), () => this.handleFetchComplete());
    this.fetchNextItem();
    this.buffer.push(item);
  }

  handleFetchComplete() {
    this.isFetching = false;
    this.fetchNextItem();
  }

  fetchNextItem() {
    if(this.isFetching) {
      // we can't fetch concurrently
      return;
    }
    // We could avoid looking through this list but tracking unfetched indices is a pain and I think
    // the time it takes is pretty trivial compared to the time we spend waiting for logs. 
    const nextUnfetched = this.buffer.find(i => !i.promise);
    if(nextUnfetched) {
    this.isFetching = true;
    nextUnfetched.fetch();
    }
  }
}

class AsyncMinHeap {
  getCompValueFn;
  heap = [];

  constructor(getCompValueFn) {
    this.getCompValueFn = getCompValueFn;
  }

  length() {
    return this.heap.length;
  }

  async pop() {
    const heap = this.heap;
    if (!heap.length) {
      return null;
    }

    [heap[0], heap[heap.length - 1]] = [heap[heap.length - 1], heap[0]];
    const top = heap.pop();

    //reheapify
    let curr = 0;
    while (2 * curr + 1 < heap.length) {
      const leftChild = 2 * curr + 1;
      const rightChild = 2 * curr + 2;
      // find smallest child
      // These inline awaits are ugly be we know we proactively buffered the logs so it should be fine
      const minChild =
        rightChild < heap.length &&
        (await this.getCompValueFn(heap[rightChild])) <
          (await this.getCompValueFn(heap[leftChild]))
          ? rightChild
          : leftChild;

      // if smallest child is smaller than curr swap, else we have a heap
      if (
        (await this.getCompValueFn(heap[minChild])) <
        (await this.getCompValueFn(heap[curr]))
      ) {
        [heap[minChild], heap[curr]] = [heap[curr], heap[minChild]];
        curr = minChild;
      } else {
        break;
      }
    }

    return top;
  }

  async push(key) {
    const heap = this.heap;
    heap.push(key);
    let curr = heap.length - 1;

    while (curr > 0) {
      let parent = Math.floor((curr - 1) / 2);
      if (
        (await this.getCompValueFn(heap[curr])) <
        (await this.getCompValueFn(heap[parent]))
      ) {
        [heap[curr], heap[parent]] = [heap[parent], heap[curr]];
        curr = parent;
      } else {
        // if no swap, break, since we heap is stable now
        break;
      }
    }
  }
}
