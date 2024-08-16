# node-linqjs

This package provides LINQ-like functionality for JavaScript, allowing you to perform complex queries and transformations on arrays and other iterable objects.

  <div align="center">
  <p>
    <a href="https://github.com/matteotorna/node-linqjs/releases/latest">
  <img src="https://img.shields.io/github/v/release/matteotorna/node-linqjs?style=for-the-badge" alt="GitHub release (latest SemVer)" /></a>
    <a href="https://github.com/matteotorna/node-linqjs/releases/latest">
    <img src="https://img.shields.io/github/release-date/matteotorna/node-linqjs?label=latest%20release&style=for-the-badge" alt="Latest release" /></a>
   <a href="https://www.npmjs.com/package/node-linqjs"><img src="https://img.shields.io/npm/v/node-linqjs.svg?logo=npm&style=for-the-badge" alt="npm version" /></a>
    <a href="https://www.npmjs.com/package/node-linqjs"><img src="https://img.shields.io/npm/dt/node-linqjs.svg?style=for-the-badge" alt="NPM downloads" /></a>
  </p>
</div>

## Installation

```bash
npm install node-linqjs
```
## Usage

### CDN script

`unpkg`

```javascript
<script src="https://unpkg.com/node-linqjs-minified@1.1.0/dist/node-linqjs-minified.min.js"></script>
 ```

 `jsdelivr`

```javascript
<script src="https://cdn.jsdelivr.net/npm/node-linqjs-minified@1.1.0/dist/node-linqjs-minified.min.js"></script>
 ```

## Node

```javascript
const { linq } = require('node-linqjs');

// Create a LINQ-enabled collection
const numbers = linq([1, 2, 3, 4, 5]);
 ```

## API Documentation

### where(predicate)

Filters a sequence of values based on a predicate.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
const evenNumbers = numbers.where(x => x % 2 === 0).toArray();
console.log(evenNumbers); // [2, 4]
 ```
In this example, we filter the array to keep only even numbers.

### select(selector)

Projects each element of a sequence into a new form.

```javascript
const numbers = linq([1, 2, 3]);
const squared = numbers.select(x => x * x).toArray();
console.log(squared); // [1, 4, 9]
 ```
Here, we transform each number by squaring it.

### selectMany(selector)

Projects each element of a sequence to an Enumerable and flattens the resulting sequences into one sequence.

```javascript
const families = linq([
  { name: 'Smiths', members: ['John', 'Jane', 'Sam'] },
  { name: 'Johnsons', members: ['Bob', 'Mary'] }
]);
const allMembers = families.selectMany(f => f.members).toArray();
console.log(allMembers); // ['John', 'Jane', 'Sam', 'Bob', 'Mary']
 ```
This example flattens the nested arrays of family members into a single array.

### orderBy(keySelector)

Sorts the elements of a sequence in ascending order according to a key.

```javascript
const people = linq([
  { name: 'Alice', age: 25 },
  { name: 'Bob', age: 30 },
  { name: 'Charlie', age: 20 }
]);
const sortedByAge = people.orderBy(p => p.age).toArray();
console.log(sortedByAge);
// [{ name: 'Charlie', age: 20 }, { name: 'Alice', age: 25 }, { name: 'Bob', age: 30 }]
 ```
This sorts the people array by age in ascending order.

### orderByDescending(keySelector)

Sorts the elements of a sequence in descending order according to a key.

```javascript
const numbers = linq([3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5]);
const sortedDescending = numbers.orderByDescending(x => x).toArray();
console.log(sortedDescending); // [9, 6, 5, 5, 5, 4, 3, 3, 2, 1, 1]
 ```
This example sorts the numbers in descending order.

### take(count)

Returns a specified number of contiguous elements from the start of a sequence.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
const firstThree = numbers.take(3).toArray();
console.log(firstThree); // [1, 2, 3]
 ```
This takes the first three elements from the array.

### takeWhile(predicate)

Returns elements from a sequence as long as a specified condition is true.

```javascript
const numbers = linq([1, 2, 3, 4, 5, 1, 2]);
const takeWhileLessThan4 = numbers.takeWhile(x => x < 4).toArray();
console.log(takeWhileLessThan4); // [1, 2, 3]
 ```
This takes elements while they are less than 4.

### skip(count

Bypasses a specified number of elements in a sequence and then returns the remaining elements.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
const skipFirstTwo = numbers.skip(2).toArray();
console.log(skipFirstTwo); // [3, 4, 5]
 ```
This skips the first two elements and returns the rest.

### skipWhile(predicate)

Bypasses elements in a sequence as long as a specified condition is true and then returns the remaining elements.

```javascript
const numbers = linq([1, 2, 3, 4, 5, 1, 2]);
const skipWhileLessThan3 = numbers.skipWhile(x => x < 3).toArray();
console.log(skipWhileLessThan3); // [3, 4, 5, 1, 2]
 ```
This skips elements while they are less than 3, then returns the rest.

### first(predicate = null)

Returns the first element of a sequence, or the first element satisfying a condition if provided.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
console.log(numbers.first()); // 1
console.log(numbers.first(x => x > 3)); // 4
 ```
The first example returns the first element, while the second returns the first element greater than 3.


### firstOrDefault(predicate = null, defaultValue = null)

Returns the first element of a sequence, or a default value if no element is found.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
console.log(numbers.firstOrDefault(x => x > 10, 0)); // 0
 ```
This returns 0 because no element is greater than 10.

### last(predicate = null)

Returns the last element of a sequence, or the last element satisfying a condition if provided.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
console.log(numbers.last()); // 5
console.log(numbers.last(x => x < 4)); // 3
 ```
The first example returns the last element, while the second returns the last element less than 4.

### lastOrDefault(predicate = null, defaultValue = null)

Returns the last element of a sequence, or a default value if no element is found.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
console.log(numbers.lastOrDefault(x => x > 10, 0)); // 0
 ```
This returns 0 because no element is greater than 10.

### single(predicate = null)

Returns the only element of a sequence, or the only element satisfying a condition if provided.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
console.log(numbers.single(x => x === 3)); // 3
 ```

### singleOrDefault(predicate = null, defaultValue = null)

Returns the only element of a sequence, or a default value if no element is found or more than one element exists.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
console.log(numbers.singleOrDefault(x => x > 10, 0)); // 0
 ```

### elementAt(index)

Returns the element at a specified index in a sequence.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
console.log(numbers.elementAt(2)); // 3
```

### elementAtOrDefault(index, defaultValue = null)

Returns the element at a specified index in a sequence or a default value if the index is out of range.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
console.log(numbers.elementAtOrDefault(10, 0)); // 0
```

### sum(selector = x => x)

Computes the sum of a sequence of numeric values.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
console.log(numbers.sum()); // 15

const items = linq([{ value: 10 }, { value: 20 }, { value: 30 }]);
console.log(items.sum(x => x.value)); // 60
```

### average(selector = x => x)

Computes the average of a sequence of numeric values.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
console.log(numbers.average()); // 3

const items = linq([{ value: 10 }, { value: 20 }, { value: 30 }]);
console.log(items.average(x => x.value)); // 20
```

### min(selector = x => x)

Returns the minimum value in a sequence of values.

```javascript
const numbers = linq([3, 1, 4, 1, 5, 9, 2, 6]);
console.log(numbers.min()); // 1

const items = linq([{ value: 10 }, { value: 20 }, { value: 5 }]);
console.log(items.min(x => x.value)); // 5
```

### max(selector = x => x)

Returns the maximum value in a sequence of values.

```javascript
const numbers = linq([3, 1, 4, 1, 5, 9, 2, 6]);
console.log(numbers.max()); // 9

const items = linq([{ value: 10 }, { value: 20 }, { value: 5 }]);
console.log(items.max(x => x.value)); // 20
```

### groupBy(keySelector, elementSelector = x => x)

Groups the elements of a sequence according to a specified key selector function.


```javascript
const people = linq([
  { name: 'Alice', age: 25 },
  { name: 'Bob', age: 30 },
  { name: 'Charlie', age: 25 },
  { name: 'David', age: 30 }
]);

const groupedByAge = people.groupBy(
  p => p.age,
  p => p.name
).toArray();

console.log(groupedByAge);
// [
//   [25, ['Alice', 'Charlie']],
//   [30, ['Bob', 'David']]
// ]
```

This groups people by their age, selecting only their names for the grouped elements.

### distinct(comparer = (a, b) => a === b)

Returns distinct elements from a sequence.

```javascript
const numbers = linq([1, 2, 2, 3, 3, 3, 4, 5, 5]);
console.log(numbers.distinct().toArray()); // [1, 2, 3, 4, 5]

const people = linq([
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' },
  { id: 1, name: 'Alice' }
]);
console.log(people.distinct((a, b) => a.id === b.id).toArray());
// [{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }]
```

### union(other, comparer = (a, b) => a === b)

Produces the set union of two sequences.

```javascript
const numbers1 = linq([1, 2, 3]);
const numbers2 = linq([3, 4, 5]);
console.log(numbers1.union(numbers2).toArray()); // [1, 2, 3, 4, 5]

const people1 = linq([{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }]);
const people2 = linq([{ id: 2, name: 'Bob' }, { id: 3, name: 'Charlie' }]);
console.log(people1.union(people2, (a, b) => a.id === b.id).toArray());
// [{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }, { id: 3, name: 'Charlie' }]
```

### intersect(other, comparer = (a, b) => a === b)

Produces the set intersection of two sequences.

```javascript
const numbers1 = linq([1, 2, 3, 4]);
const numbers2 = linq([3, 4, 5, 6]);
console.log(numbers1.intersect(numbers2).toArray()); // [3, 4]

const people1 = linq([{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }]);
const people2 = linq([{ id: 2, name: 'Bob' }, { id: 3, name: 'Charlie' }]);
console.log(people1.intersect(people2, (a, b) => a.id === b.id).toArray());
// [{ id: 2, name: 'Bob' }]
```

The first example finds the intersection of two number sequences, while the second finds the intersection of two people sequences based on their id.

### except(other, comparer = (a, b) => a === b)

Produces the set difference of two sequences.

```javascript
const numbers1 = linq([1, 2, 3, 4]);
const numbers2 = linq([3, 4, 5, 6]);
console.log(numbers1.except(numbers2).toArray()); // [1, 2]

const people1 = linq([{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }]);
const people2 = linq([{ id: 2, name: 'Bob' }, { id: 3, name: 'Charlie' }]);
console.log(people1.except(people2, (a, b) => a.id === b.id).toArray());
// [{ id: 1, name: 'Alice' }]
```

### concat(other)

Concatenates two sequences.

```javascript
const numbers1 = linq([1, 2, 3]);
const numbers2 = linq([4, 5, 6]);
console.log(numbers1.concat(numbers2).toArray()); // [1, 2, 3, 4, 5, 6]
```

### reverse()

Inverts the order of the elements in a sequence.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
console.log(numbers.reverse().toArray()); // [5, 4, 3, 2, 1]
```

### zip(other, resultSelector)

Applies a specified function to the corresponding elements of two sequences, producing a sequence of the results.

```javascript
const numbers = linq([1, 2, 3]);
const letters = linq(['A', 'B', 'C']);
const zipped = numbers.zip(letters, (n, l) => `${n}${l}`).toArray();
console.log(zipped); // ['1A', '2B', '3C']
```

### join(inner, outerKeySelector, innerKeySelector, resultSelector)

Correlates the elements of two sequences based on matching keys.

```javascript
const departments = linq([
  { id: 1, name: 'IT' },
  { id: 2, name: 'HR' }
]);
const employees = linq([
  { name: 'John', departmentId: 1 },
  { name: 'Jane', departmentId: 2 },
  { name: 'Bob', departmentId: 1 }
]);

const result = departments.join(
  employees,
  dept => dept.id,
  emp => emp.departmentId,
  (dept, emp) => ({ employeeName: emp.name, departmentName: dept.name })
).toArray();

console.log(result);
// [
//   { employeeName: 'John', departmentName: 'IT' },
//   { employeeName: 'Bob', departmentName: 'IT' },
//   { employeeName: 'Jane', departmentName: 'HR' }
// ]
```

### groupJoin(inner, outerKeySelector, innerKeySelector, resultSelector)

Correlates the elements of two sequences based on matching keys and groups the results.

```javascript
const departments = linq([
  { id: 1, name: 'IT' },
  { id: 2, name: 'HR' }
]);
const employees = linq([
  { name: 'John', departmentId: 1 },
  { name: 'Jane', departmentId: 2 },
  { name: 'Bob', departmentId: 1 },
  { name: 'Alice', departmentId: 3 }
]);

const result = departments.groupJoin(
  employees,
  dept => dept.id,
  emp => emp.departmentId,
  (dept, emps) => ({
    department: dept.name,
    employees: emps.select(e => e.name).toArray()
  })
).toArray();

console.log(result);
// [
//   { department: 'IT', employees: ['John', 'Bob'] },
//   { department: 'HR', employees: ['Jane'] }
// ]
```

### defaultIfEmpty(defaultValue = null)

Returns the elements of the specified sequence or the specified value in a singleton collection if the sequence is empty.

```javascript
const emptySequence = linq([]);
console.log(emptySequence.defaultIfEmpty('Empty').toArray()); // ['Empty']

const nonEmptySequence = linq([1, 2, 3]);
console.log(nonEmptySequence.defaultIfEmpty('Empty').toArray()); // [1, 2, 3]
```

### all(predicate)

Determines whether all elements of a sequence satisfy a condition.

```javascript
const numbers = linq([2, 4, 6, 8]);
console.log(numbers.all(x => x % 2 === 0)); // true

const mixedNumbers = linq([2, 4, 5, 8]);
console.log(mixedNumbers.all(x => x % 2 === 0)); // false
```

### any(predicate = null)

Determines whether any element of a sequence satisfies a condition.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
console.log(numbers.any(x => x > 3)); // true
console.log(numbers.any(x => x > 10)); // false
console.log(numbers.any()); // true (sequence is not empty)

const emptySequence = linq([]);
console.log(emptySequence.any()); // false
```

### contains(value, comparer = (a, b) => a === b)

Determines whether a sequence contains a specified element.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
console.log(numbers.contains(3)); // true
console.log(numbers.contains(6)); // false

const people = linq([{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }]);
console.log(people.contains({ id: 1, name: 'Alice' }, (a, b) => a.id === b.id)); // true
```

### count(predicate = null)

Returns the number of elements in a sequence that satisfy a condition.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
console.log(numbers.count()); // 5
console.log(numbers.count(x => x % 2 === 0)); // 2
```

### aggregate(seed, func, resultSelector = x => x)

Applies an accumulator function over a sequence.

```javascript
const numbers = linq([1, 2, 3, 4]);
const sum = numbers.aggregate(0, (acc, x) => acc + x);
console.log(sum); // 10

const sentence = linq(['Hello', 'World', '!']);
const fullSentence = sentence.aggregate('', (acc, word) => `${acc} ${word}`.trim());
console.log(fullSentence); // "Hello World !"
```

### toSet()

Converts the sequence to a Set.

```javascript
const numbers = linq([1, 2, 2, 3, 3, 3]);
console.log(numbers.toSet()); // Set(3) { 1, 2, 3 }
```

### toMap(keySelector = (x, i) => i, valueSelector = x => x)

Converts the sequence to a Map.

```javascript
const people = linq([
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' }
]);
const map = people.toMap(p => p.id, p => p.name);
console.log(map); // Map(2) { 1 => 'Alice', 2 => 'Bob' }
```

### toDictionary(keySelector, valueSelector = x => x)

Converts the sequence to an object (dictionary).

```javascript
const people = linq([
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' }
]);
const dict = people.toDictionary(p => p.id, p => p.name);
console.log(dict); // { '1': 'Alice', '2': 'Bob' }
```

### toLookup(keySelector, valueSelector = x => x)

Creates a lookup (dictionary of arrays) from a sequence.

```javascript
const orders = linq([
  { customerId: 1, total: 100 },
  { customerId: 2, total: 200 },
  { customerId: 1, total: 300 }
]);
const lookup = orders.toLookup(o => o.customerId, o => o.total);
console.log(lookup);
// {
//   '1': [100, 300],
//   '2': [200]
// }
```

### cast()

Casts the elements of a sequence to the specified type.

```javascript
// Example for Number
const numberExample = linq(['1', '2', '3', '4']).cast(Number).toArray();
console.log('Number:', numberExample); // [1, 2, 3, 4]

// Example for String
const stringExample = linq([1, 2, 3, 4]).cast(String).toArray();
console.log('String:', stringExample); // ['1', '2', '3', '4']

// Example for Boolean
const booleanExample = linq([1, 0, 'true', '', null, undefined]).cast(Boolean).toArray();
console.log('Boolean:', booleanExample); // [true, false, true, false, false, false]

// Example for BigInt
const bigIntExample = linq(['1', '2', '3', '4']).cast(BigInt).toArray();
console.log('BigInt:', bigIntExample); // [1n, 2n, 3n, 4n]

// Example for Symbol
const symbolExample = linq(['a', 'b', 'c']).cast(Symbol).toArray();
console.log('Symbol:', symbolExample); // [Symbol(a), Symbol(b), Symbol(c)]

// Example for Object
const objectExample = linq([1, 'two', true]).cast(Object).toArray();
console.log('Object:', objectExample); // [Number(1), String('two'), Boolean(true)]

// Example for Array
const arrayExample = linq([1, [2, 3], 4]).cast(Array).toArray();
console.log('Array:', arrayExample); // [[1], [2, 3], [4]]

// Example for Function
const functionExample = linq([1, () => 'hello']).cast(Function).toArray();
console.log('Function:', functionExample[0](), functionExample[1]()); // 1, 'hello'

// Example for Date
const dateExample = linq(['2023-01-01', new Date('2023-02-01')]).cast(Date).toArray();
console.log('Date:', dateExample); // [Date object, Date object]

// Example for RegExp
const regExpExample = linq(['abc', /def/]).cast(RegExp).toArray();
console.log('RegExp:', regExpExample); // [/abc/, /def/]

// Example for Error
const errorExample = linq(['Error 1', new Error('Error 2')]).cast(Error).toArray();
console.log('Error:', errorExample[0].message, errorExample[1].message); // 'Error 1', 'Error 2'

// Example for a custom class
class Person {
  constructor(name) {
    this.name = name;
  }
}
const personExample = linq(['Alice', 'Bob']).cast(Person).toArray();
console.log('Person:', personExample[0].name, personExample[1].name); // 'Alice', 'Bob'

// Example for an unhandled type (returns the original element)
const unknownTypeExample = linq([1, 2, 3]).cast(Symbol.iterator).toArray();
console.log('Unknown Type:', unknownTypeExample); // [1, 2, 3]
```

### ofType(type)

Filters the elements of a sequence based on a specified type.

```javascript
const mixedTypes = linq([1, '2', 3, '4', true, { id: 5 }]);
const numbers = mixedTypes.ofType('number').toArray();
console.log(numbers); // [1, 3]
```

### chunk(size)

Splits the elements of a sequence into chunks of the specified size.

```javascript
const numbers = linq([1, 2, 3, 4, 5, 6, 7]);
const chunks = numbers.chunk(3).toArray();
console.log(chunks); // [[1, 2, 3], [4, 5, 6], [7]]
```

### prepend(element)

Adds an element to the beginning of the sequence.

```javascript
const numbers = linq([2, 3, 4]);
const newSequence = numbers.prepend(1).toArray();
console.log(newSequence); // [1, 2, 3, 4]
```

### append(element)

Adds an element to the end of the sequence.

```javascript
const numbers = linq([1, 2, 3]);
const newSequence = numbers.append(4).toArray();
console.log(newSequence); // [1, 2, 3, 4]
```

### forEach(action)

Performs the specified action on each element of the sequence.

```javascript
const numbers = linq([1, 2, 3]);
numbers.forEach(x => console.log(x));
// Output:
// 1
// 2
// 3
```

### distinctBy(keySelector)

Returns distinct elements from a sequence based on a key selector function.

```javascript
const people = linq([
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' },
  { id: 1, name: 'Alice (duplicate)' }
]);
const distinctPeople = people.distinctBy(p => p.id).toArray();
console.log(distinctPeople);
// [
//   { id: 1, name: 'Alice' },
//   { id: 2, name: 'Bob' }
// ]
```

### skipLast(count)

Bypasses a specified number of elements at the end of the sequence.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
const result = numbers.skipLast(2).toArray();
console.log(result); // [1, 2, 3]
```

### takeLast(count)

Returns a new sequence that contains the last n elements from the input sequence.

```javascript
const numbers = linq([1, 2, 3, 4, 5]);
const result = numbers.takeLast(3).toArray();
console.log(result); // [3, 4, 5]
```

### sequenceEqual(other, comparer = (a, b) => a === b)

Determines whether two sequences are equal by comparing their elements using a specified equality comparer.

```javascript
// Simple number sequence comparison
const numbers1 = linq([1, 2, 3, 4, 5]);
const numbers2 = linq([1, 2, 3, 4, 5]);
const numbers3 = linq([1, 2, 3, 5, 4]);

console.log(numbers1.sequenceEqual(numbers2)); // true
console.log(numbers1.sequenceEqual(numbers3)); // false

// String sequence comparison (case-sensitive)
const words1 = linq(['apple', 'banana', 'cherry']);
const words2 = linq(['apple', 'banana', 'cherry']);
const words3 = linq(['Apple', 'Banana', 'Cherry']);

console.log(words1.sequenceEqual(words2)); // true
console.log(words1.sequenceEqual(words3)); // false

// String sequence comparison (case-insensitive)
console.log(words1.sequenceEqual(words3, (a, b) => a.toLowerCase() === b.toLowerCase())); // true

// Object sequence comparison
const people1 = linq([
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' },
  { id: 3, name: 'Charlie' }
]);

const people2 = linq([
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' },
  { id: 3, name: 'Charlie' }
]);

const people3 = linq([
  { id: 1, name: 'Alice' },
  { id: 3, name: 'Charlie' },
  { id: 2, name: 'Bob' }
]);

// Compare objects by their 'id' property
const compareById = (a, b) => a.id === b.id;

console.log(people1.sequenceEqual(people2, compareById)); // true
console.log(people1.sequenceEqual(people3, compareById)); // false

// Compare objects by both 'id' and 'name' properties
const compareByIdAndName = (a, b) => a.id === b.id && a.name === b.name;

console.log(people1.sequenceEqual(people2, compareByIdAndName)); // true
console.log(people1.sequenceEqual(people3, compareByIdAndName)); // false

// Comparison with different lengths
const numbers4 = linq([1, 2, 3, 4, 5, 6]);
console.log(numbers1.sequenceEqual(numbers4)); // false
```

This expanded example demonstrates:

- Comparing simple number sequences.
- Comparing string sequences with case-sensitive and case-insensitive comparisons.
- Comparing object sequences using custom comparison functions.
- Showing how the order of elements matters.
- Demonstrating that sequences of different lengths are never equal.

The sequenceEqual method is useful when you need to compare two sequences for exact equality, considering both the values and the order of the elements. The optional comparer function allows for custom equality comparisons, which is particularly useful when dealing with complex objects or when you need to implement specific comparison logic.

### parallelWhere(predicate, workerCount)

Filters the elements of a sequence in parallel based on a predicate.

```javascript
const { linq } = require('node-linqjs');

(async function() {
  try {
    const numbers = linq(Array.from({ length: 1000000 }, (_, i) => i));

    // parallelWhere
    const evenNumbers = await numbers.toAsync().parallelWhere(x => x % 2 === 0).toArray();
    console.log(evenNumbers[0], evenNumbers[1]);

  } catch (error) {
    console.error("An error occurred:", error);
  }
})();
```

### parallelSelect(selector, workerCount)

Projects each element of a sequence into a new form in parallel.

```javascript
const { linq } = require('node-linqjs');

(async function() {
  try {
    const numbers = linq(Array.from({ length: 1000000 }, (_, i) => i));

    // parallelSelect
    const squaredNumbers = await numbers.toAsync().parallelSelect(x => x * x).toArray();
    console.log(squaredNumbers[1], squaredNumbers[10]);

  } catch (error) {
    console.error("An error occurred:", error);
  }
})();
```

### parallelAggregate(func, seed, workerCount)

Applies an accumulator function over a sequence in parallel.

```javascript
const { linq } = require('node-linqjs');

(async function() {
  try {
    const numbers = linq(Array.from({ length: 1000000 }, (_, i) => i));

    // parallelAggregate
    const sum = await numbers.toAsync().parallelAggregate((acc, x) => acc + x, 0);
    console.log(sum);

  } catch (error) {
    console.error("An error occurred:", error);
  }
})();
```

## Example of all parallel functions

```javascript
const { linq } = require('node-linqjs');

(async function() {
  try {
    // Create an array of objects representing people
    const people = Array.from({ length: 1000000 }, (_, i) => ({
      id: i,
      name: `Person ${i}`,
      age: Math.floor(Math.random() * 100),
      salary: Math.floor(Math.random() * 100000) + 20000
    }));

    const linqPeople = linq(people);

    // Use parallelWhere to filter people over 30 years old
    const over30 = await linqPeople
      .toAsync()
      .parallelWhere(p => p.age > 30)
      .toArray();

    console.log(`Number of people over 30 years old: ${over30.length}`);

    // Use parallelSelect to calculate taxes (assuming 20% of salary)
    const taxes = await linqPeople
      .toAsync()
      .parallelSelect(p => ({
        id: p.id,
        name: p.name,
        tax: p.salary * 0.2
      }))
      .toArray();

    console.log(`Taxes for person 1000:`, taxes[1000]);

    // Use parallelAggregate to calculate total salary
    const totalSalary = await linqPeople
      .toAsync()
      .parallelAggregate((acc, p) => acc + p.salary, 0);

    console.log(`Total salary: $${totalSalary.toLocaleString()}`);

    // Combine multiple operations: find the average age of people earning more than 50000
    const averageAgeHighEarners = await linqPeople
      .toAsync()
      .parallelWhere(p => p.salary > 50000)
      .parallelSelect(p => p.age)
      .toArray()
      .then(ages => ages.reduce((sum, age) => sum + age, 0) / ages.length);

    console.log(`Average age of people earning more than $50,000: ${averageAgeHighEarners.toFixed(2)}`);

  } catch (error) {
    console.error("An error occurred:", error);
  }
})();
```

## Static Methods

### Enumerable.range(start, count)

Generates a sequence of integral numbers within a specified range.

```javascript
const { Enumerable } = require('node-linqjs');

const numbers = Enumerable.range(1, 5);
console.log(numbers.toArray()); // [1, 2, 3, 4, 5]
```

### Enumerable.repeat(element, count)

Generates a sequence that contains one repeated value.

```javascript
const { Enumerable } = require('node-linqjs');

const repeatedSequence = Enumerable.repeat('A', 3);
console.log(repeatedSequence.toArray()); // ['A', 'A', 'A']
```