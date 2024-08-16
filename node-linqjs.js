const { Worker } = require('worker_threads');
const os = require('os');

class Enumerable {
  constructor(source) {
    this.source = source;
  }

  [Symbol.iterator]() {
    return this.source[Symbol.iterator]();
  }

  static from(source) {
    return new Enumerable(source);
  }

  where(predicate) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        for (const item of self) {
          if (predicate(item)) {
            yield item;
          }
        }
      }
    });
  }

  select(selector) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        for (const item of self) {
          yield selector(item);
        }
      }
    });
  }

  selectMany(selector) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        for (const item of self) {
          yield* selector(item);
        }
      }
    });
  }

  orderBy(keySelector) {
    return new OrderedEnumerable(this, keySelector, false);
  }

  orderByDescending(keySelector) {
    return new OrderedEnumerable(this, keySelector, true);
  }

  take(count) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        let taken = 0;
        for (const item of self) {
          if (taken >= count) break;
          yield item;
          taken++;
        }
      }
    });
  }

  takeWhile(predicate) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        for (const item of self) {
          if (!predicate(item)) break;
          yield item;
        }
      }
    });
  }

  skip(count) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        let skipped = 0;
        for (const item of self) {
          if (skipped >= count) {
            yield item;
          } else {
            skipped++;
          }
        }
      }
    });
  }

  skipWhile(predicate) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        let skipping = true;
        for (const item of self) {
          if (skipping && !predicate(item)) {
            skipping = false;
          }
          if (!skipping) {
            yield item;
          }
        }
      }
    });
  }

  first(predicate = null) {
    for (const item of this) {
      if (predicate === null || predicate(item)) {
        return item;
      }
    }
    throw new Error("Sequence contains no matching elements");
  }

  firstOrDefault(predicate = null, defaultValue = null) {
    try {
      return this.first(predicate);
    } catch {
      return defaultValue;
    }
  }

  last(predicate = null) {
    let lastItem = undefined;
    let found = false;
    for (const item of this) {
      if (predicate === null || predicate(item)) {
        lastItem = item;
        found = true;
      }
    }
    if (found) return lastItem;
    throw new Error("Sequence contains no matching elements");
  }

  lastOrDefault(predicate = null, defaultValue = null) {
    try {
      return this.last(predicate);
    } catch {
      return defaultValue;
    }
  }

  single(predicate = null) {
    let found = false;
    let result = undefined;
    for (const item of this) {
      if (predicate === null || predicate(item)) {
        if (found) throw new Error("Sequence contains more than one matching element");
        result = item;
        found = true;
      }
    }
    if (!found) throw new Error("Sequence contains no matching elements");
    return result;
  }

  singleOrDefault(predicate = null, defaultValue = null) {
    try {
      return this.single(predicate);
    } catch (e) {
      if (e.message === "Sequence contains no matching elements") {
        return defaultValue;
      }
      throw e;
    }
  }

  elementAt(index) {
    let currentIndex = 0;
    for (const item of this) {
      if (currentIndex === index) return item;
      currentIndex++;
    }
    throw new Error("Index out of range");
  }

  elementAtOrDefault(index, defaultValue = null) {
    try {
      return this.elementAt(index);
    } catch {
      return defaultValue;
    }
  }

  sum(selector = x => x) {
    let sum = 0;
    for (const item of this) {
      sum += selector(item);
    }
    return sum;
  }

  average(selector = x => x) {
    let sum = 0;
    let count = 0;
    for (const item of this) {
      sum += selector(item);
      count++;
    }
    if (count === 0) throw new Error("Sequence contains no elements");
    return sum / count;
  }

  min(selector = x => x) {
    let min = undefined;
    for (const item of this) {
      const value = selector(item);
      if (min === undefined || value < min) {
        min = value;
      }
    }
    if (min === undefined) throw new Error("Sequence contains no elements");
    return min;
  }

  max(selector = x => x) {
    let max = undefined;
    for (const item of this) {
      const value = selector(item);
      if (max === undefined || value > max) {
        max = value;
      }
    }
    if (max === undefined) throw new Error("Sequence contains no elements");
    return max;
  }

  groupBy(keySelector, elementSelector = x => x) {
    const groups = new Map();
    for (const item of this) {
      const key = keySelector(item);
      const element = elementSelector(item);
      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key).push(element);
    }
    return new Enumerable(groups);
  }

  distinct(comparer = (a, b) => a === b) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        const set = new Set();
        for (const item of self) {
          if (!Array.from(set).some(x => comparer(x, item))) {
            set.add(item);
            yield item;
          }
        }
      }
    });
  }

  union(other, comparer = (a, b) => a === b) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        const set = new Set();
        for (const item of self) {
          if (!Array.from(set).some(x => comparer(x, item))) {
            set.add(item);
            yield item;
          }
        }
        for (const item of other) {
          if (!Array.from(set).some(x => comparer(x, item))) {
            set.add(item);
            yield item;
          }
        }
      }
    });
  }

  intersect(other, comparer = (a, b) => a === b) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        const otherArray = Array.from(other);
        const set = new Set();
        for (const item of self) {
          if (otherArray.some(x => comparer(x, item)) &&
            !Array.from(set).some(x => comparer(x, item))) {
            set.add(item);
            yield item;
          }
        }
      }
    });
  }

  except(other, comparer = (a, b) => a === b) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        const otherArray = Array.from(other);
        const set = new Set();
        for (const item of self) {
          if (!otherArray.some(x => comparer(x, item)) &&
            !Array.from(set).some(x => comparer(x, item))) {
            set.add(item);
            yield item;
          }
        }
      }
    });
  }

  concat(other) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        yield* self;
        yield* other;
      }
    });
  }

  reverse() {
    return new Enumerable([...this].reverse());
  }

  sequenceEqual(other, comparer = (a, b) => a === b) {
    const otherIterator = other[Symbol.iterator]();
    for (const item of this) {
      const { value, done } = otherIterator.next();
      if (done || !comparer(item, value)) return false;
    }
    return otherIterator.next().done;
  }

  zip(other, resultSelector) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        const iteratorA = self[Symbol.iterator]();
        const iteratorB = other[Symbol.iterator]();
        while (true) {
          const a = iteratorA.next();
          const b = iteratorB.next();
          if (a.done || b.done) break;
          yield resultSelector(a.value, b.value);
        }
      }
    });
  }

  join(inner, outerKeySelector, innerKeySelector, resultSelector) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        const innerArray = Array.from(inner);
        for (const outerItem of self) {
          const outerKey = outerKeySelector(outerItem);
          for (const innerItem of innerArray) {
            if (outerKey === innerKeySelector(innerItem)) {
              yield resultSelector(outerItem, innerItem);
            }
          }
        }
      }
    });
  }

  groupJoin(inner, outerKeySelector, innerKeySelector, resultSelector) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        const innerArray = Array.from(inner);
        for (const outerItem of self) {
          const outerKey = outerKeySelector(outerItem);
          const innerGroup = innerArray.filter(innerItem =>
            outerKey === innerKeySelector(innerItem)
          );
          yield resultSelector(outerItem, new Enumerable(innerGroup));
        }
      }
    });
  }

  defaultIfEmpty(defaultValue = null) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        let hasElements = false;
        for (const item of self) {
          hasElements = true;
          yield item;
        }
        if (!hasElements) {
          yield defaultValue;
        }
      }
    });
  }

  all(predicate) {
    for (const item of this) {
      if (!predicate(item)) return false;
    }
    return true;
  }

  any(predicate = null) {
    for (const item of this) {
      if (predicate === null || predicate(item)) {
        return true;
      }
    }
    return false;
  }

  contains(value, comparer = (a, b) => a === b) {
    for (const item of this) {
      if (comparer(item, value)) return true;
    }
    return false;
  }

  count(predicate = null) {
    let count = 0;
    for (const item of this) {
      if (predicate === null || predicate(item)) {
        count++;
      }
    }
    return count;
  }

  aggregate(seed, func, resultSelector = x => x) {
    let result = seed;
    for (const item of this) {
      result = func(result, item);
    }
    return resultSelector(result);
  }

  toArray() {
    return [...this];
  }

  toSet() {
    return new Set(this);
  }

  toMap(keySelector = (x, i) => i, valueSelector = x => x) {
    const map = new Map();
    let index = 0;
    for (const item of this) {
      map.set(keySelector(item, index), valueSelector(item, index));
      index++;
    }
    return map;
  }

  toDictionary(keySelector, valueSelector = x => x) {
    const dict = {};
    for (const item of this) {
      const key = keySelector(item);
      if (key in dict) {
        throw new Error("Duplicate key");
      }
      dict[key] = valueSelector(item);
    }
    return dict;
  }

  toLookup(keySelector, valueSelector = x => x) {
    const lookup = {};
    for (const item of this) {
      const key = keySelector(item);
      if (!(key in lookup)) {
        lookup[key] = [];
      }
      lookup[key].push(valueSelector(item));
    }
    return lookup;
  }

  cast(type) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        for (const item of self) {
          if (type === Number) {
            yield Number(item);
          } else if (type === String) {
            yield String(item);
          } else if (type === Boolean) {
            yield Boolean(item);
          } else if (type === BigInt) {
            yield BigInt(item);
          } else if (type === Symbol) {
            yield Symbol(item.toString());
          } else if (type === Object) {
            yield Object(item);
          } else if (type === Array) {
            yield Array.isArray(item) ? item : [item];
          } else if (type === Function) {
            yield typeof item === 'function' ? item : () => item;
          } else if (type === Date) {
            yield item instanceof Date ? item : new Date(item);
          } else if (type === RegExp) {
            yield item instanceof RegExp ? item : new RegExp(item);
          } else if (type === Error) {
            yield item instanceof Error ? item : new Error(item);
          } else if (typeof type === 'function') {
            // for pensonalize functions
            try {
              yield new type(item);
            } catch (e) {             
              yield type(item);
            }
          } else {
            // give the original type
            yield item;
          }
        }
      }
    });
  }

  ofType(type) {
    return this.where(item => typeof item === type);
  }

  chunk(size) {
    if (size <= 0) throw new Error("Size must be greater than 0");
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        let chunk = [];
        for (const item of self) {
          chunk.push(item);
          if (chunk.length === size) {
            yield chunk;
            chunk = [];
          }
        }
        if (chunk.length > 0) {
          yield chunk;
        }
      }
    });
  }

  prepend(element) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        yield element;
        yield* self;
      }
    });
  }

  append(element) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        yield* self;
        yield element;
      }
    });
  }

  forEach(action) {
    for (const item of this) {
      action(item);
    }
  }

  aggregate(func) {
    const iterator = this[Symbol.iterator]();
    const first = iterator.next();
    if (first.done) {
      throw new Error("Sequence contains no elements");
    }
    let result = first.value;
    for (const item of this) {
      result = func(result, item);
    }
    return result;
  }

  sequenceEqual(second, comparer = (a, b) => a === b) {
    const secondIterator = second[Symbol.iterator]();
    for (const item of this) {
      const { value, done } = secondIterator.next();
      if (done || !comparer(item, value)) return false;
    }
    return secondIterator.next().done;
  }

  static empty() {
    return new Enumerable([]);
  }

  static range(start, count) {
    if (count < 0) throw new Error("Count must be non-negative");
    return new Enumerable({
      *[Symbol.iterator]() {
        for (let i = 0; i < count; i++) {
          yield start + i;
        }
      }
    });
  }

  static repeat(element, count) {
    if (count < 0) throw new Error("Count must be non-negative");
    return new Enumerable({
      *[Symbol.iterator]() {
        for (let i = 0; i < count; i++) {
          yield element;
        }
      }
    });
  }

  distinctBy(keySelector) {
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        const seenKeys = new Set();
        for (const item of self) {
          const key = keySelector(item);
          if (!seenKeys.has(key)) {
            seenKeys.add(key);
            yield item;
          }
        }
      }
    });
  }

  skipLast(count) {
    if (count < 0) throw new Error("Count must be non-negative");
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        const buffer = [];
        for (const item of self) {
          buffer.push(item);
          if (buffer.length > count) {
            yield buffer.shift();
          }
        }
      }
    });
  }

  takeLast(count) {
    if (count < 0) throw new Error("Count must be non-negative");
    const self = this;
    return new Enumerable({
      *[Symbol.iterator]() {
        const buffer = [];
        for (const item of self) {
          buffer.push(item);
          if (buffer.length > count) {
            buffer.shift();
          }
        }
        yield* buffer;
      }
    });
  }

  toAsync() {
    return new AsyncEnumerable(this);
  }
  
}

class AsyncEnumerable {
  constructor(source) {
    this.source = source;
  }

  [Symbol.asyncIterator]() {
    if (this.source[Symbol.asyncIterator]) {
      return this.source[Symbol.asyncIterator]();
    } else if (this.source[Symbol.iterator]) {
      const iterator = this.source[Symbol.iterator]();
      return {
        async next() {
          const { done, value } = iterator.next();
          return { done, value };
        }
      };
    } else {
      throw new Error("Source is not iterable");
    }
  }

  async toArray() {
    const result = [];
    for await (const item of this) {
      result.push(item);
    }
    return result;
  }

  parallelSelect(selector, workerCount = os.cpus().length) {
    const self = this;
    return new AsyncEnumerable({
      [Symbol.asyncIterator]: async function* () {
        const items = await self.toArray();
        const chunkSize = Math.ceil(items.length / workerCount);
        const chunks = Array.from({ length: workerCount }, (_, i) =>
          items.slice(i * chunkSize, (i + 1) * chunkSize)
        );

        const workerScript = `
          const { parentPort, workerData } = require('worker_threads');
          const [chunk, selectorString] = workerData;
          const selector = new Function('return ' + selectorString)();
          const result = chunk.map(selector);
          parentPort.postMessage(result);
        `;

        const promises = chunks.map((chunk, index) =>
          new Promise((resolve, reject) => {
            const worker = new Worker(workerScript, {
              eval: true,
              workerData: [chunk, selector.toString()]
            });
            worker.on('message', resolve);
            worker.on('error', reject);
            worker.on('exit', (code) => {
              if (code !== 0)
                reject(new Error(`Worker stopped with exit code ${code}`));
            });
          })
        );

        for (const promise of promises) {
          const result = await promise;
          yield* result;
        }
      }
    });
  }

  parallelWhere(predicate, workerCount = os.cpus().length) {
    const self = this;
    return new AsyncEnumerable({
      [Symbol.asyncIterator]: async function* () {
        const items = await self.toArray();
        const chunkSize = Math.ceil(items.length / workerCount);
        const chunks = Array.from({ length: workerCount }, (_, i) =>
          items.slice(i * chunkSize, (i + 1) * chunkSize)
        );

        const workerScript = `
          const { parentPort, workerData } = require('worker_threads');
          const [chunk, predicateString] = workerData;
          const predicate = new Function('return ' + predicateString)();
          const result = chunk.filter(predicate);
          parentPort.postMessage(result);
        `;

        const promises = chunks.map((chunk, index) =>
          new Promise((resolve, reject) => {
            const worker = new Worker(workerScript, {
              eval: true,
              workerData: [chunk, predicate.toString()]
            });
            worker.on('message', resolve);
            worker.on('error', reject);
            worker.on('exit', (code) => {
              if (code !== 0)
                reject(new Error(`Worker stopped with exit code ${code}`));
            });
          })
        );

        for (const promise of promises) {
          const result = await promise;
          yield* result;
        }
      }
    });
  }

  async parallelAggregate(func, seed, workerCount = os.cpus().length) {
    const items = await this.toArray();
    const chunkSize = Math.ceil(items.length / workerCount);
    const chunks = Array.from({ length: workerCount }, (_, i) =>
      items.slice(i * chunkSize, (i + 1) * chunkSize)
    );

    const workerScript = `
      const { parentPort, workerData } = require('worker_threads');
      const [chunk, funcString, seed] = workerData;
      const func = new Function('return ' + funcString)();
      const result = chunk.reduce(func, seed);
      parentPort.postMessage(result);
    `;

    const promises = chunks.map((chunk, index) =>
      new Promise((resolve, reject) => {
        const worker = new Worker(workerScript, {
          eval: true,
          workerData: [chunk, func.toString(), seed]
        });
        worker.on('message', resolve);
        worker.on('error', reject);
        worker.on('exit', (code) => {
          if (code !== 0)
            reject(new Error(`Worker stopped with exit code ${code}`));
        });
      })
    );

    const results = await Promise.all(promises);
    return results.reduce(func, seed);
  }
}

class OrderedEnumerable extends Enumerable {
  constructor(source, keySelector, descending = false) {
    super(source);
    this.keySelectors = [{ keySelector, descending }];
  }

  thenBy(keySelector) {
    this.keySelectors.push({ keySelector, descending: false });
    return this;
  }

  thenByDescending(keySelector) {
    this.keySelectors.push({ keySelector, descending: true });
    return this;
  }

  [Symbol.iterator]() {
    const array = [...this.source];
    array.sort((a, b) => {
      for (const { keySelector, descending } of this.keySelectors) {
        const keyA = keySelector(a);
        const keyB = keySelector(b);
        if (keyA < keyB) return descending ? 1 : -1;
        if (keyA > keyB) return descending ? -1 : 1;
      }
      return 0;
    });
    return array[Symbol.iterator]();
  }
}

// Extend Array prototype
Object.getOwnPropertyNames(Enumerable.prototype).forEach(name => {
  if (name !== 'constructor' && typeof Enumerable.prototype[name] === 'function') {
    Array.prototype[name] = function (...args) {
      return Enumerable.from(this)[name](...args);
    };
  }
});

function linq(iterable) {
  return new Enumerable(iterable);
}

module.exports = {
  linq,
  Enumerable
};